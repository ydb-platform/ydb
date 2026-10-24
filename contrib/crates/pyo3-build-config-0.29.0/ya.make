# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pyo3_build_config)

CRATE_VERSION(0.29.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_ENV(PYO3_CONFIG_FILE=${ARCADIA_ROOT}/contrib/crates/pyo3-build-config-0.29.0/python-config.txt)

CRATE_SOURCES(
    src/errors.rs
    src/impl_.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    python-config.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    extension-module
)

PEERDIR(
    contrib/crates/target-lexicon-0.13.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-build-config/pyo3-build-config-0.29.0.crate)

END()
