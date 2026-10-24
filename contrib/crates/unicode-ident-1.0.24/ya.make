# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(unicode_ident)

CRATE_VERSION(1.0.24)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/tables.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitattributes
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-UNICODE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("(MIT OR Apache-2.0) AND Unicode-3.0")
VERSION(1.0.24)
ORIGINAL_SOURCE(https://static.crates.io/crates/unicode-ident/unicode-ident-1.0.24.crate)

END()
