# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(opentelemetry_otlp)

CRATE_VERSION(0.32.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/exporter/http/logs.rs
    src/exporter/http/metrics.rs
    src/exporter/http/mod.rs
    src/exporter/http/trace.rs
    src/exporter/mod.rs
    src/exporter/tonic/logs.rs
    src/exporter/tonic/metrics.rs
    src/exporter/tonic/mod.rs
    src/exporter/tonic/trace.rs
    src/logs.rs
    src/metric.rs
    src/retry.rs
    src/retry_classification.rs
    src/span.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    README.md
    allowed-external-types.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    grpc-tonic
    http
    http-proto
    metrics
    opentelemetry-http
    prost
    reqwest
    reqwest-blocking-client
    tls-ring
    tokio
    tonic
    tonic-types
    trace
)

PEERDIR(
    contrib/crates/http-1.5.0
    contrib/crates/opentelemetry-0.32.0
    contrib/crates/opentelemetry-http-0.32.0
    contrib/crates/opentelemetry-proto-0.32.0
    contrib/crates/opentelemetry_sdk-0.32.1
    contrib/crates/prost-0.14.4
    contrib/crates/reqwest-0.13.4
    contrib/crates/thiserror-2.0.21
    contrib/crates/tokio-1.54.1
    contrib/crates/tonic-0.14.6
    contrib/crates/tonic-types-0.14.6
)

LICENSE("Apache-2.0")
VERSION(0.32.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/opentelemetry-otlp/opentelemetry-otlp-0.32.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/async-trait-0.1.92
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/displaydoc-0.2.7
    contrib/crates/futures-macro-0.3.34
    contrib/crates/pin-project-internal-1.1.13
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerocopy-derive-0.8.63
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
