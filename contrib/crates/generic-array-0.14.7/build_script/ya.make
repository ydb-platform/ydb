# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.14.7)
CRATE_EDITION(2015)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/arr.rs
    ../src/functional.rs
    ../src/hex.rs
    ../src/impl_serde.rs
    ../src/impl_zeroize.rs
    ../src/impls.rs
    ../src/iter.rs
    ../src/lib.rs
    ../src/sequence.rs
)

CRATE_INPUTS(
    ../CHANGELOG.md
    ../LICENSE
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    more_lengths
)

PEERDIR(
    contrib/crates/version_check-0.9.5
)

LICENSE("MIT")
VERSION(0.14.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/generic-array/generic-array-0.14.7.crate)

END()
