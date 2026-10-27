# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(ident_case)

CRATE_VERSION(1.0.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .travis.yml
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/ident_case/ident_case-1.0.1.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
