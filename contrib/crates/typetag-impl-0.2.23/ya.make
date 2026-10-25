# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(typetag_impl)

CRATE_VERSION(0.2.23)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/parse.rs
    src/tagged_impl.rs
    src/tagged_trait.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
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
VERSION(0.2.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/typetag-impl/typetag-impl-0.2.23.crate)

CRATE_SOURCES(
    src/lib.rs
    src/parse.rs
    src/tagged_impl.rs
    src/tagged_trait.rs
)

END()
