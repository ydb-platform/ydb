# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(icu_normalizer)

CRATE_VERSION(2.2.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/harfbuzz.rs
    src/properties.rs
    src/provider.rs
    src/uts46.rs
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
    compiled_data
)

PEERDIR(
    contrib/crates/icu_collections-2.2.0
    contrib/crates/icu_normalizer_data-2.2.0
    contrib/crates/icu_provider-2.2.0
    contrib/crates/smallvec-1.15.2
    contrib/crates/zerovec-0.11.6
)

LICENSE("Unicode-3.0")
VERSION(2.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_normalizer/icu_normalizer-2.2.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.6
    contrib/crates/yoke-derive-0.8.2
    contrib/crates/zerofrom-derive-0.1.7
    contrib/crates/zerovec-derive-0.11.3
)

END()
