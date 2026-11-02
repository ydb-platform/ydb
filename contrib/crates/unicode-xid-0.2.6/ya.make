# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(unicode_xid)

CRATE_VERSION(0.2.6)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/tables.rs
    src/tests.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    COPYRIGHT
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/unicode-xid/unicode-xid-0.2.6.crate)

CRATE_SOURCES(
    src/lib.rs
    src/tables.rs
    src/tests.rs
)

END()
