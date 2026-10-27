# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tracing_serde)

CRATE_VERSION(0.2.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/fields.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/serde-1.0.229
    contrib/crates/tracing-core-0.1.36
)

LICENSE("MIT")
VERSION(0.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/tracing-serde/tracing-serde-0.2.0.crate)

CRATE_FEATURES(
    valuable
    valuable-serde
    valuable_crate
)

PEERDIR(
    contrib/crates/valuable-0.1.1
    contrib/crates/valuable-serde-0.1.1
)

CRATE_SOURCES(
    src/fields.rs
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/valuable-derive-0.1.1
)

END()
