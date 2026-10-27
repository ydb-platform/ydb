# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(stable_deref_trait)

CRATE_VERSION(1.2.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.2.1)
LICENSE("MIT OR Apache-2.0")


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

CRATE_SOURCES(
    src/lib.rs
)

END()
