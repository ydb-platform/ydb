# Native Rust build with dependencies shared through contrib/crates.

CRATE(icu_normalizer)

CRATE_VERSION(2.4.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/harfbuzz.rs
    src/properties.rs
    src/provider.rs
    src/uts46.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    compiled_data
)

PEERDIR(
    contrib/crates/icu_collections-2.4.0
    contrib/crates/icu_normalizer_data-2.4.0
    contrib/crates/icu_provider-2.4.0
    contrib/crates/smallvec-1.16.4
    contrib/crates/zerovec-0.11.9
)

LICENSE("Unicode-3.0")
VERSION(2.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_normalizer/icu_normalizer-2.4.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/serde_derive-1.0.229
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
