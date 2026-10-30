# Native Rust build with dependencies shared through contrib/crates.

CRATE(pyo3_build_config)

CRATE_VERSION(0.29.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)


CRATE_SOURCES(
    src/errors.rs
    src/impl_.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
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
VERSION(0.29.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-build-config/pyo3-build-config-0.29.2.crate)

END()
