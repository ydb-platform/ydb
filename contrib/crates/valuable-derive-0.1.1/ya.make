# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(valuable_derive)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/attr.rs
    src/error.rs
    src/expand.rs
)

CRATE_INPUTS(
    Cargo.lock
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT")
VERSION(0.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/valuable-derive/valuable-derive-0.1.1.crate)

END()
