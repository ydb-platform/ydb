# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(prometheus)

CRATE_VERSION(0.14.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    proto/mod.rs
    proto/proto_model.rs
    src/atomic64.rs
    src/auto_flush.rs
    src/counter.rs
    src/desc.rs
    src/encoder/mod.rs
    src/encoder/pb.rs
    src/encoder/text.rs
    src/errors.rs
    src/gauge.rs
    src/histogram.rs
    src/macros.rs
    src/metrics.rs
    src/nohash.rs
    src/plain_model.rs
    src/process_collector.rs
    src/proto_ext.rs
    src/pulling_gauge.rs
    src/push.rs
    src/registry.rs
    src/timer.rs
    src/value.rs
    src/vec.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    CODE_OF_CONDUCT.md
    Cargo.lock
    LICENSE
    Makefile
    README.md
    docs/release.md
    proto/proto_model.proto
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cfg-if-1.0.4
    contrib/crates/fnv-1.0.7
    contrib/crates/lazy_static-1.5.0
    contrib/crates/memchr-2.8.3
    contrib/crates/parking_lot-0.12.5
    contrib/crates/thiserror-2.0.19
)

CRATE_BUILD_SCRIPT(contrib/crates/prometheus-0.14.0/build_script)

LICENSE("Apache-2.0")
VERSION(0.14.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/prometheus/prometheus-0.14.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/thiserror-impl-2.0.19
)

END()

RECURSE(build_script)
