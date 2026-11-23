# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(zerofrom_derive)

CRATE_VERSION(0.1.8)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/visitor.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.9
    contrib/crates/synstructure-0.14.0
)

LICENSE("Unicode-3.0")
VERSION(0.1.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerofrom-derive/zerofrom-derive-0.1.8.crate)

END()
