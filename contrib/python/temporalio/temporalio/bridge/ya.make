# Native Rust build; versions and features follow temporalio 1.32.0 Cargo.lock.

CRATE(temporal_sdk_bridge)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/client.rs
    src/client_rpc_generated.rs
    src/envconfig.rs
    src/metric.rs
    src/runtime.rs
    src/testing.rs
    src/worker.rs
)

CRATE_INPUTS(
    Cargo.lock
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    tls-ring
)

PEERDIR(
    contrib/crates/anyhow-1.0.104
    contrib/crates/futures-0.3.34
    contrib/crates/prost-0.14.4
    contrib/crates/pyo3-0.29.2
    contrib/crates/pyo3-async-runtimes-0.29.0
    contrib/crates/pythonize-0.29.0
    contrib/crates/tokio-1.54.1
    contrib/crates/tokio-rustls-0.26.6
    contrib/crates/tokio-stream-0.1.19
    contrib/crates/tonic-0.14.6
    contrib/crates/tracing-0.1.44
    contrib/crates/url-2.5.8
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/client
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/common
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/sdk-core
)

LICENSE(MIT)
VERSION(0.1.0)

CRATE_PROC_MACROS(
    contrib/crates/async-trait-0.1.92
    contrib/crates/bon-macros-3.9.3
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/darling_macro-0.23.0
    contrib/crates/derive_more-impl-2.1.1
    contrib/crates/displaydoc-0.2.7
    contrib/crates/enum-iterator-derive-1.5.0
    contrib/crates/enum_dispatch-0.3.13
    contrib/crates/futures-macro-0.3.34
    contrib/crates/mockall_derive-0.14.0
    contrib/crates/pin-project-internal-1.1.13
    contrib/crates/prost-derive-0.14.4
    contrib/crates/pyo3-macros-0.29.2
    contrib/crates/rustversion-1.0.23
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-1.0.69
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
    contrib/crates/typetag-impl-0.2.23
    contrib/crates/valuable-derive-0.1.1
    contrib/crates/windows-implement-0.60.2
    contrib/crates/windows-interface-0.59.3
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerocopy-derive-0.8.63
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/macros
)

END()

RECURSE(
    sdk-core/crates/client
    sdk-core/crates/common
    sdk-core/crates/common-wasm
    sdk-core/crates/macros
    sdk-core/crates/protos
    sdk-core/crates/sdk-core
)
