# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(either)

CRATE_VERSION(1.17.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/into_either.rs
    src/iterator.rs
    src/serde_untagged.rs
    src/serde_untagged_optional.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README-crates.io.md
    README.rst
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
    use_std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.17.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/either/either-1.17.0.crate)

END()
