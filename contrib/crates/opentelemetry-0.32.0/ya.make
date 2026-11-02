# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(opentelemetry)

CRATE_VERSION(0.32.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/baggage.rs
    src/common.rs
    src/context.rs
    src/context/future_ext.rs
    src/global/internal_logging.rs
    src/global/metrics.rs
    src/global/mod.rs
    src/global/propagation.rs
    src/global/trace.rs
    src/logs/logger.rs
    src/logs/mod.rs
    src/logs/noop.rs
    src/logs/record.rs
    src/metrics/instruments/counter.rs
    src/metrics/instruments/gauge.rs
    src/metrics/instruments/histogram.rs
    src/metrics/instruments/mod.rs
    src/metrics/instruments/up_down_counter.rs
    src/metrics/meter.rs
    src/metrics/mod.rs
    src/metrics/noop.rs
    src/propagation/composite.rs
    src/propagation/mod.rs
    src/propagation/text_map_propagator.rs
    src/testing/mod.rs
    src/testing/trace.rs
    src/trace/context.rs
    src/trace/mod.rs
    src/trace/noop.rs
    src/trace/span.rs
    src/trace/span_context.rs
    src/trace/tracer.rs
    src/trace/tracer_provider.rs
    src/trace_context.rs
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
    futures
    futures-core
    futures-sink
    metrics
    pin-project-lite
    thiserror
    trace
)

PEERDIR(
    contrib/crates/futures-core-0.3.34
    contrib/crates/futures-sink-0.3.34
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/thiserror-2.0.21
)

LICENSE("Apache-2.0")
VERSION(0.32.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/opentelemetry/opentelemetry-0.32.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/thiserror-impl-2.0.21
)

END()
