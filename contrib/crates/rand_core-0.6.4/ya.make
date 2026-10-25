# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(rand_core)

CRATE_VERSION(0.6.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.6.4)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/block.rs
    src/error.rs
    src/impls.rs
    src/le.rs
    src/os.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    COPYRIGHT
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_FEATURES(
    alloc
    getrandom
    std
)

PEERDIR(
    contrib/crates/getrandom-0.2.17
)

CRATE_SOURCES(
    src/block.rs
    src/error.rs
    src/impls.rs
    src/le.rs
    src/lib.rs
    src/os.rs
)

END()
