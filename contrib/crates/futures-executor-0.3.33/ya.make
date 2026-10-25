# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(futures_executor)

CRATE_VERSION(0.3.33)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/enter.rs
    src/local_pool.rs
    src/thread_pool.rs
    src/unpark_mutex.rs
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
    default
    std
)

PEERDIR(
    contrib/crates/futures-core-0.3.33
    contrib/crates/futures-task-0.3.33
    contrib/crates/futures-util-0.3.33
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.33)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-executor/futures-executor-0.3.33.crate)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.33
)

END()
