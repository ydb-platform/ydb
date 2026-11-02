# Native Rust build with dependencies shared through contrib/crates.

CRATE(crossbeam_utils)

CRATE_VERSION(0.8.23)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build-common.rs
    build.rs
    no_atomic.rs
    src/atomic/atomic_cell.rs
    src/atomic/consume.rs
    src/atomic/mod.rs
    src/atomic/seq_lock.rs
    src/atomic/seq_lock_wide.rs
    src/backoff.rs
    src/cache_padded.rs
    src/sync/mod.rs
    src/sync/once_lock.rs
    src/sync/parker.rs
    src/sync/sharded_lock.rs
    src/sync/wait_group.rs
    src/thread.rs
)

CRATE_INPUTS(
    CHANGELOG.md
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

CRATE_BUILD_SCRIPT(contrib/crates/crossbeam-utils-0.8.23/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/crossbeam-utils/crossbeam-utils-0.8.23.crate)

END()

RECURSE(build_script)
