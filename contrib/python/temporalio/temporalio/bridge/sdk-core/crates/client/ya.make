# Native Rust build; versions and features follow temporalio 1.32.0 Cargo.lock.

CRATE(temporalio_client)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/activity.rs
    src/activity/activity_execution_info.rs
    src/activity/activity_handle.rs
    src/async_activity_handle.rs
    src/callback_based.rs
    src/dns.rs
    src/envconfig.rs
    src/errors.rs
    src/grpc.rs
    src/interceptors.rs
    src/metrics.rs
    src/options_structs.rs
    src/plugins.rs
    src/proxy.rs
    src/replaceable.rs
    src/request_extensions.rs
    src/retry.rs
    src/rpc_options.rs
    src/schedules.rs
    src/test_helpers.rs
    src/worker.rs
    src/workflow_handle.rs
    src/workflow_status.rs
)

CRATE_INPUTS(
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    core-based-sdk
    envconfig
    tls-ring
)

PEERDIR(
    contrib/crates/anyhow-1.0.104
    contrib/crates/backon-1.6.0
    contrib/crates/base64-0.22.1
    contrib/crates/bon-3.9.3
    contrib/crates/bytes-1.12.1
    contrib/crates/derive_more-2.1.1
    contrib/crates/dyn-clone-1.0.20
    contrib/crates/futures-retry-0.6.0
    contrib/crates/futures-util-0.3.34
    contrib/crates/http-1.5.0
    contrib/crates/http-body-util-0.1.5
    contrib/crates/hyper-1.12.0
    contrib/crates/hyper-util-0.1.21
    contrib/crates/parking_lot-0.12.5
    contrib/crates/rand-0.10.3
    contrib/crates/serde_json-1.0.154
    contrib/crates/thiserror-2.0.21
    contrib/crates/tokio-1.54.1
    contrib/crates/tokio-rustls-0.26.6
    contrib/crates/tonic-0.14.6
    contrib/crates/tower-0.5.3
    contrib/crates/tracing-0.1.44
    contrib/crates/url-2.5.8
    contrib/crates/uuid-1.29.0
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/common
)

LICENSE(MIT)
VERSION(0.6.0)

CRATE_PROC_MACROS(
    contrib/crates/async-trait-0.1.92
    contrib/crates/bon-macros-3.9.3
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/darling_macro-0.23.0
    contrib/crates/derive_more-impl-2.1.1
    contrib/crates/displaydoc-0.2.7
    contrib/crates/futures-macro-0.3.34
    contrib/crates/pin-project-internal-1.1.13
    contrib/crates/prost-derive-0.14.4
    contrib/crates/rustversion-1.0.23
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-1.0.69
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
    contrib/crates/typetag-impl-0.2.23
    contrib/crates/valuable-derive-0.1.1
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerocopy-derive-0.8.63
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
