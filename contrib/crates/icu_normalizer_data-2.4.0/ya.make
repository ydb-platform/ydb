# Native Rust build with dependencies shared through contrib/crates.

CRATE(icu_normalizer_data)

CRATE_VERSION(2.4.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    data/mod.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    data/normalizer_nfc_v1.rs.data
    data/normalizer_nfd_data_v1.rs.data
    data/normalizer_nfd_supplement_v1.rs.data
    data/normalizer_nfd_tables_v1.rs.data
    data/normalizer_nfkd_data_v1.rs.data
    data/normalizer_nfkd_tables_v1.rs.data
    data/normalizer_uts46_data_v1.rs.data
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/icu_normalizer_data-2.4.0/build_script)

LICENSE("Unicode-3.0")
VERSION(2.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_normalizer_data/icu_normalizer_data-2.4.0.crate)

END()

RECURSE(build_script)
