# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(7.3.0)
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
    ../Cargo.lock
    ../LICENSE
    ../Readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

LICENSE("BSD-3-Clause")
VERSION(7.3.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/zstd-safe/zstd-safe-7.3.0.crate)

END()
