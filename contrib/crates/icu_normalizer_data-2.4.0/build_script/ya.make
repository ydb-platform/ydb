# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(2.4.0)
CRATE_EDITION(2024)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../data/mod.rs
    ../src/lib.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE
    ../README.md
    ../data/normalizer_nfc_v1.rs.data
    ../data/normalizer_nfd_data_v1.rs.data
    ../data/normalizer_nfd_supplement_v1.rs.data
    ../data/normalizer_nfd_tables_v1.rs.data
    ../data/normalizer_nfkd_data_v1.rs.data
    ../data/normalizer_nfkd_tables_v1.rs.data
    ../data/normalizer_uts46_data_v1.rs.data
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Unicode-3.0")
VERSION(2.4.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/icu_normalizer_data/icu_normalizer_data-2.4.0.crate)

END()
