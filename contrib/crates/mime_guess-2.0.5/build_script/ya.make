# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(2.0.5)
CRATE_EDITION(2015)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/impl_bin_search.rs
    ../src/impl_phf.rs
    ../src/lib.rs
    ../src/mime_types.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    rev-mappings
)

PEERDIR(
    contrib/crates/unicase-2.10.0
)

LICENSE("MIT")
VERSION(2.0.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/mime_guess/mime_guess-2.0.5.crate)

END()
