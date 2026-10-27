# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(parking_lot)

CRATE_VERSION(0.12.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.12.5)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/condvar.rs
    src/deadlock.rs
    src/elision.rs
    src/fair_mutex.rs
    src/mutex.rs
    src/once.rs
    src/raw_fair_mutex.rs
    src/raw_mutex.rs
    src/raw_rwlock.rs
    src/remutex.rs
    src/rwlock.rs
    src/util.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    bors.toml
    release-plz.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)
PEERDIR(
    contrib/crates/lock_api-0.4.14
    contrib/crates/parking_lot_core-0.9.12
)

CRATE_FEATURES(
    arc_lock
    send_guard
    serde
)

CRATE_SOURCES(
    src/condvar.rs
    src/deadlock.rs
    src/elision.rs
    src/fair_mutex.rs
    src/lib.rs
    src/mutex.rs
    src/once.rs
    src/raw_fair_mutex.rs
    src/raw_mutex.rs
    src/raw_rwlock.rs
    src/remutex.rs
    src/rwlock.rs
    src/util.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
