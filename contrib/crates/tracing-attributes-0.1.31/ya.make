# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(tracing_attributes)

CRATE_VERSION(0.1.31)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/attr.rs
    src/expand.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
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

LICENSE("MIT")
VERSION(0.1.31)
ORIGINAL_SOURCE(https://static.crates.io/crates/tracing-attributes/tracing-attributes-0.1.31.crate)

CRATE_SOURCES(
    src/attr.rs
    src/expand.rs
    src/lib.rs
)

END()
