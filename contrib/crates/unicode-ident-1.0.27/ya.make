# Native Rust build with dependencies shared through contrib/crates.

CRATE(unicode_ident)

CRATE_VERSION(1.0.27)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/tables.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-UNICODE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("(MIT OR Apache-2.0) AND Unicode-3.0")
VERSION(1.0.27)
ORIGINAL_SOURCE(https://static.crates.io/crates/unicode-ident/unicode-ident-1.0.27.crate)

END()
