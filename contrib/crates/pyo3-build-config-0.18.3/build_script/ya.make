# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.18.3)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_ENV(PYO3_CONFIG_FILE=${ARCADIA_ROOT}/contrib/crates/pyo3-build-config-0.18.3/python-config.txt)

CRATE_SOURCES(
    ../src/errors.rs
    ../src/impl_.rs
    ../src/import_lib.rs
    ../src/lib.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../LICENSE
    ../python-config.txt
)

CRATE_FEATURES(
    abi3
    abi3-py310
    abi3-py311
    abi3-py37
    abi3-py38
    abi3-py39
    default
    extension-module
    resolve-config
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/target-lexicon-0.12.16
)

LICENSE("Apache-2.0")
VERSION(0.18.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-build-config/pyo3-build-config-0.18.3.crate)

CRATE_SOURCES(
    ../src/errors.rs
    ../src/impl_.rs
    ../src/import_lib.rs
)

CRATE_SOURCES(
    ../src/lib.rs
)

END()
