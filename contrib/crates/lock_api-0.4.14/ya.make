# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(lock_api)

CRATE_VERSION(0.4.14)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.4.14)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/mutex.rs
    src/remutex.rs
    src/rwlock.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    atomic_usize
    default
)
PEERDIR(
    contrib/crates/scopeguard-1.2.0
)

CRATE_FEATURES(
    arc_lock
    serde
)

PEERDIR(
    contrib/crates/serde-1.0.229
)

CRATE_SOURCES(
    src/lib.rs
    src/mutex.rs
    src/remutex.rs
    src/rwlock.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
