// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Leader-driven periodic cleanup of expired personal access tokens.
//!
//! Spawned on shard 0 of every node, but a pass acts only on the caught-up
//! metadata primary: the delete is a replicated mutation, so the leader
//! proposes it once and every replica applies the commit. Backups never
//! propose, so cleanup cannot race across the cluster.

use crate::bootstrap::ServerNgShard;
use consensus::MetadataHandle;
use iggy_binary_protocol::WireName;
use iggy_common::IggyTimestamp;
use metadata::impls::metadata::StreamsFrontend;
use shard::Receiver;
use std::rc::Rc;
use std::time::Duration;
use tracing::{info, trace};

/// Run the cleaner until `stop` fires. Wakes every `interval`; expiry is
/// wall-clock driven, so no metadata-commit wake is needed.
pub async fn run_pat_cleaner(shard: Rc<ServerNgShard>, stop: Receiver<()>, interval: Duration) {
    trace!(
        shard = shard.id,
        interval_ms = interval.as_millis(),
        "personal access token cleaner started"
    );
    loop {
        // `Ok(_)`: stop signalled -> exit. `Err(_)`: interval elapsed -> run
        // a pass, which returns `true` if it saw stop mid-batch so shutdown
        // need not wait for the next tick.
        match compio::time::timeout(interval, stop.recv()).await {
            Ok(_) => break,
            Err(_) => {
                if clean_expired_tokens(&shard, &stop).await {
                    break;
                }
            }
        }
    }
    trace!(shard = shard.id, "personal access token cleaner exited");
}

/// Run one cleanup pass. Returns `true` when shutdown was observed
/// mid-batch and the caller should stop looping, `false` otherwise.
async fn clean_expired_tokens(shard: &Rc<ServerNgShard>, stop: &Receiver<()>) -> bool {
    let metadata = shard.plane.metadata();
    if !metadata.is_caught_up_primary() {
        return false;
    }

    let now = IggyTimestamp::now();
    // The read guard is released when the closure returns, before any
    // `submit_*` await below.
    let expired = metadata
        .mux_stm
        .users()
        .read(|users| users.expired_personal_access_tokens(now));

    if expired.is_empty() {
        return false;
    }

    let mut removed = 0u32;
    let mut stop_requested = false;
    for (user_id, name) in expired {
        // Observe stop between tokens: an in-flight submit is not
        // cancel-safe, so let the current delete finish rather than cut it.
        // Bounds shutdown delay to one submit, not the whole batch.
        if stop.try_recv().is_ok() {
            stop_requested = true;
            break;
        }
        // `name` came from a stored PAT, originally a validated `WireName`,
        // so reconstruction is infallible.
        let wire_name = WireName::new(name.as_ref())
            .expect("stored PAT name was validated as a WireName at creation");
        match metadata
            .submit_delete_personal_access_token_in_process(user_id, wire_name)
            .await
        {
            Ok(_) => removed += 1,
            // Leadership lost or the node fell behind mid-pass; the next
            // primary cleans up on its own tick. Stop the batch.
            Err(error) => {
                trace!(
                    user_id,
                    ?error,
                    "stopping personal access token cleanup pass"
                );
                break;
            }
        }
    }

    if removed > 0 {
        info!(removed, "removed expired personal access tokens");
    }
    stop_requested
}
