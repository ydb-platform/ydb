# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zerotrie)

CRATE_VERSION(0.2.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/builder/branch_meta.rs
    src/builder/dense.rs
    src/builder/konst/builder.rs
    src/builder/konst/mod.rs
    src/builder/konst/store.rs
    src/builder/litemap.rs
    src/builder/mod.rs
    src/builder/nonconst/builder.rs
    src/builder/nonconst/mod.rs
    src/builder/nonconst/store.rs
    src/builder/slice_indices.rs
    src/byte_phf/builder.rs
    src/byte_phf/cached_owned.rs
    src/byte_phf/mod.rs
    src/cursor.rs
    src/dense.rs
    src/error.rs
    src/helpers.rs
    src/options.rs
    src/reader.rs
    src/serde.rs
    src/varint.rs
    src/zerotrie.rs
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
    yoke
    zerofrom
)

PEERDIR(
    contrib/crates/yoke-0.8.3
    contrib/crates/zerofrom-0.1.8
)

LICENSE("Unicode-3.0")
VERSION(0.2.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerotrie/zerotrie-0.2.4.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.6
    contrib/crates/yoke-derive-0.8.2
    contrib/crates/zerofrom-derive-0.1.7
)

END()
