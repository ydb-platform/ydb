# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(zerovec_derive)

CRATE_VERSION(0.11.3)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("Unicode-3.0")
VERSION(0.11.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerovec-derive/zerovec-derive-0.11.3.crate)

END()
