# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(7.2.4)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/constants.rs
    ../src/constants_experimental.rs
    ../src/constants_seekable.rs
    ../src/lib.rs
    ../src/seekable.rs
    ../src/tests.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE
    ../LICENSE.Apache-2.0
    ../LICENSE.Mit
    ../Readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(7.2.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/zstd-safe/zstd-safe-7.2.4.crate)

END()
