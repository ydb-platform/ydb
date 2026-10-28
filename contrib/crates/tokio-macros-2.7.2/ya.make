# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(tokio_macros)

CRATE_VERSION(2.7.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/entry.rs
    src/select.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.9
)

LICENSE("MIT")
VERSION(2.7.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/tokio-macros/tokio-macros-2.7.2.crate)

END()
