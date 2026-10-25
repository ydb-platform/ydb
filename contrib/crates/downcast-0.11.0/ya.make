# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(downcast)

CRATE_VERSION(0.11.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitattributes
    .gitignore
    Cargo.lock
    LICENSE-MIT
    README.md
    clippy.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT")
VERSION(0.11.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/downcast/downcast-0.11.0.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
