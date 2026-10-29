# Native Rust build with dependencies shared through contrib/crates.

CRATE(crossbeam_epoch)

CRATE_VERSION(0.9.21)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/atomic.rs
    src/collector.rs
    src/default.rs
    src/deferred.rs
    src/epoch.rs
    src/guard.rs
    src/internal.rs
    src/sync/list.rs
    src/sync/mod.rs
    src/sync/once_lock.rs
    src/sync/queue.rs
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
    alloc
    std
)

PEERDIR(
    contrib/crates/crossbeam-utils-0.8.23
)

CRATE_BUILD_SCRIPT(contrib/crates/crossbeam-epoch-0.9.21/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.9.21)
ORIGINAL_SOURCE(https://static.crates.io/crates/crossbeam-epoch/crossbeam-epoch-0.9.21.crate)

CRATE_FEATURES(
    default
)

END()

RECURSE(build_script)
