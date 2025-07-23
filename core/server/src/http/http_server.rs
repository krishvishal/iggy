/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::configs::http::{HttpConfig, HttpCorsConfig};
use crate::http::diagnostics::request_diagnostics;
use crate::http::http_shard_wrapper::HttpSafeShard;
use crate::http::jwt::cleaner::start_expired_tokens_cleaner;
use crate::http::jwt::jwt_manager::JwtManager;
use crate::http::jwt::middleware::jwt_auth;
use crate::http::metrics::metrics;
use crate::http::shared::AppState;
use crate::http::*;
use crate::shard::IggyShard;
// use crate::streaming::systems::system::SharedSystem;
use axum::extract::DefaultBodyLimit;
use axum::extract::connect_info::Connected;
use axum::http::Method;
use axum::{Router, middleware};
use axum_server::tls_rustls::RustlsConfig;
use compio_net::TcpListener;
use iggy_common::IggyError;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
pub struct CompioSocketAddr(pub SocketAddr);

impl From<SocketAddr> for CompioSocketAddr {
    fn from(addr: SocketAddr) -> Self {
        CompioSocketAddr(addr)
    }
}

impl From<CompioSocketAddr> for SocketAddr {
    fn from(addr: CompioSocketAddr) -> Self {
        addr.0
    }
}

impl<'a> Connected<cyper_axum::IncomingStream<'a, TcpListener>> for CompioSocketAddr {
    fn connect_info(target: cyper_axum::IncomingStream<'a, TcpListener>) -> Self {
        let addr = *target.remote_addr();
        CompioSocketAddr(addr)
    }
}

/// Starts the HTTP API server.
/// Returns the address the server is listening on.
pub async fn start(config: HttpConfig, shard: Rc<IggyShard>) -> Result<(), IggyError> {
    if shard.id != 0 {
        info!(
            "HTTP server disabled for shard {} (only runs on shard 0)",
            shard.id
        );
        panic!("HTTP server only runs on shard 0");
    }

    let api_name = if config.tls.enabled {
        "HTTP API (TLS)"
    } else {
        "HTTP API"
    };

    let app_state = build_app_state(&config, shard).await;
    let mut app = Router::new()
        .merge(system::router(app_state.clone(), &config.metrics))
        .merge(personal_access_tokens::router(app_state.clone()))
        .merge(users::router(app_state.clone()))
        .merge(streams::router(app_state.clone()))
        .merge(topics::router(app_state.clone()))
        .merge(consumer_groups::router(app_state.clone()))
        .merge(consumer_offsets::router(app_state.clone()))
        .merge(partitions::router(app_state.clone()))
        .merge(messages::router(app_state.clone()))
        .layer(DefaultBodyLimit::max(
            config.max_request_size.as_bytes_u64() as usize,
        ))
        .layer(middleware::from_fn_with_state(app_state.clone(), jwt_auth));

    if config.cors.enabled {
        app = app.layer(configure_cors(config.cors));
    }

    if config.metrics.enabled {
        app = app.layer(middleware::from_fn_with_state(app_state.clone(), metrics));
    }

    start_expired_tokens_cleaner(app_state.clone());
    app = app.layer(middleware::from_fn(request_diagnostics));

    if !config.tls.enabled {
        let listener = TcpListener::bind(config.address.clone())
            .await
            .unwrap_or_else(|_| panic!("Failed to bind to HTTP address {}", config.address));
        let address = listener
            .local_addr()
            .expect("Failed to get local address for HTTP server");
        info!("Started {api_name} on: {address}");
        compio::runtime::spawn(async move {
            if let Err(error) = cyper_axum::serve(
                listener,
                app.into_make_service_with_connect_info::<CompioSocketAddr>(),
            )
            .await
            {
                error!("Failed to start {api_name} server, error {}", error);
            }
        })
        .detach();
        // address
        Ok(())
    } else {
        let tls_config = RustlsConfig::from_pem_file(
            PathBuf::from(config.tls.cert_file),
            PathBuf::from(config.tls.key_file),
        )
        .await
        .unwrap();

        let listener = std::net::TcpListener::bind(config.address).unwrap();
        let address = listener
            .local_addr()
            .expect("Failed to get local address for HTTPS / TLS server");

        info!("Started {api_name} on: {address}");

        tokio::task::spawn(async move {
            if let Err(error) = axum_server::from_tcp_rustls(listener, tls_config)
                .serve(app.into_make_service_with_connect_info::<SocketAddr>())
                .await
            {
                error!("Failed to start {api_name} server, error: {}", error);
            }
        });

        // address
        Ok(())
    }
}

async fn build_app_state(config: &HttpConfig, shard: Rc<IggyShard>) -> Arc<AppState> {
    let tokens_path;
    let persister;
    {
        tokens_path = shard.config.system.get_state_tokens_path();
        persister = shard.storage.persister.clone();
    }

    let jwt_manager = JwtManager::from_config(persister, &tokens_path, &config.jwt);
    if let Err(error) = jwt_manager {
        panic!("Failed to initialize JWT manager: {error}");
    }

    let jwt_manager = jwt_manager.unwrap();
    if jwt_manager.load_revoked_tokens().await.is_err() {
        panic!("Failed to load revoked access tokens");
    }

    Arc::new(AppState {
        jwt_manager,
        shard: HttpSafeShard::new(shard),
    })
}

fn configure_cors(config: HttpCorsConfig) -> CorsLayer {
    let allowed_origins = match config.allowed_origins {
        origins if origins.is_empty() => AllowOrigin::default(),
        origins if origins.first().unwrap() == "*" => AllowOrigin::any(),
        origins => AllowOrigin::list(origins.iter().map(|s| s.parse().unwrap())),
    };

    let allowed_headers = config
        .allowed_headers
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect::<Vec<_>>();

    let exposed_headers = config
        .exposed_headers
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| s.parse().unwrap())
        .collect::<Vec<_>>();

    let allowed_methods = config
        .allowed_methods
        .iter()
        .filter(|s| !s.is_empty())
        .map(|s| match s.to_uppercase().as_str() {
            "GET" => Method::GET,
            "POST" => Method::POST,
            "PUT" => Method::PUT,
            "DELETE" => Method::DELETE,
            "HEAD" => Method::HEAD,
            "OPTIONS" => Method::OPTIONS,
            "CONNECT" => Method::CONNECT,
            "PATCH" => Method::PATCH,
            "TRACE" => Method::TRACE,
            _ => panic!("Invalid HTTP method: {s}"),
        })
        .collect::<Vec<_>>();

    CorsLayer::new()
        .allow_methods(allowed_methods)
        .allow_origin(allowed_origins)
        .allow_headers(allowed_headers)
        .expose_headers(exposed_headers)
        .allow_credentials(config.allow_credentials)
        .allow_private_network(config.allow_private_network)
}
