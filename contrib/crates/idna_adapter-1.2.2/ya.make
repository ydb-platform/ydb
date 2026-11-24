# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(idna_adapter)

CRATE_VERSION(1.2.2)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    compiled_data
)

PEERDIR(
    contrib/crates/icu_normalizer-2.4.0
    contrib/crates/icu_properties-2.4.0
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.2.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/idna_adapter/idna_adapter-1.2.2.crate)

CRATE_SOURCES(
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/serde_derive-1.0.229
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
