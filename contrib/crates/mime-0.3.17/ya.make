# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(mime)

CRATE_VERSION(0.3.17)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/parse.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .travis.yml
    CONTRIBUTING.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.17)
ORIGINAL_SOURCE(https://static.crates.io/crates/mime/mime-0.3.17.crate)

CRATE_SOURCES(
    src/lib.rs
    src/parse.rs
)

END()
