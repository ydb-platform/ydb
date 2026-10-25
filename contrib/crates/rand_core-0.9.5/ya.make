# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rand_core)

CRATE_VERSION(0.9.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/block.rs
    src/impls.rs
    src/le.rs
    src/os.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    COPYRIGHT
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    os_rng
    std
)

PEERDIR(
    contrib/crates/getrandom-0.3.4
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.9.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand_core/rand_core-0.9.5.crate)

CRATE_SOURCES(
    src/block.rs
    src/impls.rs
    src/le.rs
    src/lib.rs
    src/os.rs
)

END()
