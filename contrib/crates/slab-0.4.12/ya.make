# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(slab)

CRATE_VERSION(0.4.12)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/builder.rs
    src/serde.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    SECURITY.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT")
VERSION(0.4.12)
ORIGINAL_SOURCE(https://static.crates.io/crates/slab/slab-0.4.12.crate)

CRATE_SOURCES(
    src/builder.rs
    src/lib.rs
    src/serde.rs
)

END()
