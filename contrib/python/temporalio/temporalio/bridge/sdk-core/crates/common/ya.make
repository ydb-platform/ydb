# Native Rust build; versions and features follow temporalio 1.32.0 Cargo.lock.

CRATE(temporalio_common)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/envconfig.rs
    src/fsm_trait.rs
    src/payload_limits.rs
    src/payload_visitor.rs
    src/protos/mod.rs
    src/telemetry.rs
    src/telemetry/log_export.rs
    src/telemetry/metrics.rs
    src/telemetry/metrics/core.rs
    src/telemetry/otel.rs
    src/telemetry/prometheus_meter.rs
    src/telemetry/prometheus_server.rs
    src/worker.rs
)

CRATE_INPUTS(
    LICENSE.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    core-telemetry-bridge
    envconfig
    otel
    prometheus
    serde_serialize
    tls-ring
)

PEERDIR(
    contrib/crates/anyhow-1.0.104
    contrib/crates/bon-3.9.3
    contrib/crates/crc32fast-1.5.3
    contrib/crates/derive_more-2.1.1
    contrib/crates/dirs-6.0.0
    contrib/crates/erased-serde-0.4.10
    contrib/crates/futures-0.3.34
    contrib/crates/futures-channel-0.3.34
    contrib/crates/http-body-util-0.1.5
    contrib/crates/hyper-1.12.0
    contrib/crates/hyper-util-0.1.21
    contrib/crates/opentelemetry-0.32.0
    contrib/crates/opentelemetry-otlp-0.32.0
    contrib/crates/opentelemetry_sdk-0.32.1
    contrib/crates/parking_lot-0.12.5
    contrib/crates/prometheus-0.14.0
    contrib/crates/prost-0.14.4
    contrib/crates/reqwest-0.13.4
    contrib/crates/ringbuf-0.5.1
    contrib/crates/rustls-0.23.46
    contrib/crates/serde-1.0.229
    contrib/crates/serde_json-1.0.154
    contrib/crates/thiserror-2.0.21
    contrib/crates/tokio-1.54.1
    contrib/crates/toml-1.1.3+spec-1.1.0
    contrib/crates/tonic-0.14.6
    contrib/crates/tracing-0.1.44
    contrib/crates/tracing-core-0.1.36
    contrib/crates/tracing-subscriber-0.3.23
    contrib/crates/url-2.5.8
    contrib/crates/uuid-1.29.0
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/common-wasm
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/protos
)

CRATE_BUILD_SCRIPT(contrib/python/temporalio/temporalio/bridge/sdk-core/crates/common/build_script)

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

RECURSE(
    build_script
)
