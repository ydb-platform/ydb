# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(zerofrom_derive)

CRATE_VERSION(0.1.7)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/visitor.rs
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
    contrib/crates/synstructure-0.13.2
)

LICENSE("Unicode-3.0")
VERSION(0.1.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerofrom-derive/zerofrom-derive-0.1.7.crate)

END()
