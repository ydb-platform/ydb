# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.8.23)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../build-common.rs
    ../no_atomic.rs
    ../src/atomic/atomic_cell.rs
    ../src/atomic/consume.rs
    ../src/atomic/mod.rs
    ../src/atomic/seq_lock.rs
    ../src/atomic/seq_lock_wide.rs
    ../src/backoff.rs
    ../src/cache_padded.rs
    ../src/lib.rs
    ../src/sync/mod.rs
    ../src/sync/once_lock.rs
    ../src/sync/parker.rs
    ../src/sync/sharded_lock.rs
    ../src/sync/wait_group.rs
    ../src/thread.rs
)

CRATE_INPUTS(
    ../CHANGELOG.md
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/crossbeam-utils/crossbeam-utils-0.8.23.crate)

END()
