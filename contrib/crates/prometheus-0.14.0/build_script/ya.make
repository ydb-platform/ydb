# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.14.0)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../proto/mod.rs
    ../proto/proto_model.rs
    ../src/atomic64.rs
    ../src/auto_flush.rs
    ../src/counter.rs
    ../src/desc.rs
    ../src/encoder/mod.rs
    ../src/encoder/pb.rs
    ../src/encoder/text.rs
    ../src/errors.rs
    ../src/gauge.rs
    ../src/histogram.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/metrics.rs
    ../src/nohash.rs
    ../src/plain_model.rs
    ../src/process_collector.rs
    ../src/proto_ext.rs
    ../src/pulling_gauge.rs
    ../src/push.rs
    ../src/registry.rs
    ../src/timer.rs
    ../src/value.rs
    ../src/vec.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../CHANGELOG.md
    ../CODE_OF_CONDUCT.md
    ../Cargo.lock
    ../LICENSE
    ../Makefile
    ../README.md
    ../docs/release.md
    ../proto/proto_model.proto
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0")
VERSION(0.14.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/prometheus/prometheus-0.14.0.crate)

CRATE_SOURCES(
    ../proto/proto_model.rs
    ../src/plain_model.rs
)

CRATE_SOURCES(
    ../src/atomic64.rs
    ../src/auto_flush.rs
    ../src/counter.rs
    ../src/desc.rs
    ../src/encoder/mod.rs
    ../src/encoder/pb.rs
    ../src/encoder/text.rs
    ../src/errors.rs
    ../src/gauge.rs
    ../src/histogram.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/metrics.rs
    ../src/nohash.rs
    ../src/process_collector.rs
    ../src/proto_ext.rs
    ../src/pulling_gauge.rs
    ../src/push.rs
    ../src/registry.rs
    ../src/timer.rs
    ../src/value.rs
    ../src/vec.rs
)

END()
