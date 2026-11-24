# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(enum_iterator_derive)

CRATE_VERSION(1.5.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

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

LICENSE("0BSD")
VERSION(1.5.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/enum-iterator-derive/enum-iterator-derive-1.5.0.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
