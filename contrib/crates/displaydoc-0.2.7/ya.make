# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(displaydoc)

CRATE_VERSION(0.2.7)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/attr.rs
    src/expand.rs
    src/fmt.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.9
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/displaydoc/displaydoc-0.2.7.crate)

END()
