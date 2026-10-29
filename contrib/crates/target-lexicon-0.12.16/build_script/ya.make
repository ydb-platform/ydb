# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.12.16)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/data_model.rs
    ../src/host.rs
    ../src/lib.rs
    ../src/parse_error.rs
    ../src/targets.rs
    ../src/triple.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE
    ../README.md
    ../scripts/rust-targets.sh
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

LICENSE("Apache-2.0 WITH LLVM-exception")
VERSION(0.12.16)
ORIGINAL_SOURCE(https://static.crates.io/crates/target-lexicon/target-lexicon-0.12.16.crate)

END()
