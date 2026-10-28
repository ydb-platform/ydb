# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(darling_macro)

CRATE_VERSION(0.23.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/darling_core-0.23.0
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT")
VERSION(0.23.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/darling_macro/darling_macro-0.23.0.crate)

END()
