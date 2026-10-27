# Native Rust build; versions and features follow temporalio 1.32.0 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/envconfig.rs
    ../src/fsm_trait.rs
    ../src/lib.rs
    ../src/payload_limits.rs
    ../src/payload_visitor.rs
    ../src/protos/mod.rs
    ../src/telemetry.rs
    ../src/telemetry/log_export.rs
    ../src/telemetry/metrics.rs
    ../src/telemetry/metrics/core.rs
    ../src/telemetry/otel.rs
    ../src/telemetry/prometheus_meter.rs
    ../src/telemetry/prometheus_server.rs
    ../src/worker.rs
)

CRATE_INPUTS(
    ../LICENSE.txt
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
    contrib/crates/prost-0.14.4
    contrib/crates/prost-types-0.14.4
)

LICENSE(MIT)
VERSION(0.6.0)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
