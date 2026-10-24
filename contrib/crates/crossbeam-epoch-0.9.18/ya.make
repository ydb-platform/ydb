# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(crossbeam_epoch)

CRATE_VERSION(0.9.18)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.9.18)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/sanitize.rs
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
    .cargo-checksum.json
    .cargo_vcs_info.json
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
PEERDIR(contrib/crates/crossbeam-utils-0.8.22)

END()
