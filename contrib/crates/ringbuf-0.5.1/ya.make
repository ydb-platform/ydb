# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(ringbuf)

CRATE_VERSION(0.5.1)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alias.rs
    src/benchmarks/base.rs
    src/benchmarks/iter.rs
    src/benchmarks/mod.rs
    src/benchmarks/parts.rs
    src/benchmarks/slice.rs
    src/rb/local.rs
    src/rb/macros.rs
    src/rb/mod.rs
    src/rb/shared.rs
    src/rb/traits.rs
    src/rb/utils.rs
    src/storage.rs
    src/tests/access.rs
    src/tests/basic.rs
    src/tests/drop.rs
    src/tests/fmt_write.rs
    src/tests/frozen.rs
    src/tests/hold.rs
    src/tests/init.rs
    src/tests/iter.rs
    src/tests/mod.rs
    src/tests/new.rs
    src/tests/overwrite.rs
    src/tests/read_write.rs
    src/tests/shared.rs
    src/tests/skip.rs
    src/tests/slice.rs
    src/tests/unsized_.rs
    src/tests/zero_sized.rs
    src/traits/consumer.rs
    src/traits/mod.rs
    src/traits/observer.rs
    src/traits/producer.rs
    src/traits/ring_buffer.rs
    src/traits/split.rs
    src/traits/utils.rs
    src/transfer.rs
    src/utils.rs
    src/wrap/caching.rs
    src/wrap/direct.rs
    src/wrap/frozen.rs
    src/wrap/mod.rs
    src/wrap/traits.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .gitlab-ci.yml
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rustfmt.toml
    scripts/bench.sh
    scripts/miri.sh
    scripts/test.sh
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

PEERDIR(
    contrib/crates/crossbeam-utils-0.8.23
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.5.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/ringbuf/ringbuf-0.5.1.crate)

END()
