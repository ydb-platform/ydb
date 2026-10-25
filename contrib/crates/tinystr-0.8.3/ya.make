# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tinystr)

CRATE_VERSION(0.8.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ascii.rs
    src/asciibyte.rs
    src/databake.rs
    src/error.rs
    src/int_ops.rs
    src/macros.rs
    src/serde.rs
    src/ule.rs
    src/unvalidated.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    zerovec
)

PEERDIR(
    contrib/crates/zerovec-0.11.6
)

LICENSE("Unicode-3.0")
VERSION(0.8.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/tinystr/tinystr-0.8.3.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.6
    contrib/crates/yoke-derive-0.8.2
    contrib/crates/zerofrom-derive-0.1.7
    contrib/crates/zerovec-derive-0.11.3
)

END()
