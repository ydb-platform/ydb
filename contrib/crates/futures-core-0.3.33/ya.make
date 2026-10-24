# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(futures_core)

CRATE_VERSION(0.3.33)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/future.rs
    src/stream.rs
    src/task/__internal/atomic_waker.rs
    src/task/__internal/mod.rs
    src/task/mod.rs
    src/task/poll.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.33)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-core/futures-core-0.3.33.crate)

END()
