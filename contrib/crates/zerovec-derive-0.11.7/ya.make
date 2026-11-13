# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(zerovec_derive)

CRATE_VERSION(0.11.7)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/make_ule.rs
    src/make_varule.rs
    src/ule.rs
    src/utils.rs
    src/varule.rs
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
)

LICENSE("Unicode-3.0")
VERSION(0.11.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerovec-derive/zerovec-derive-0.11.7.crate)

END()
