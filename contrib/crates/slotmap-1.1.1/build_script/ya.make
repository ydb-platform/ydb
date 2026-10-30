# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.1.1)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/basic.rs
    ../src/dense.rs
    ../src/hop.rs
    ../src/lib.rs
    ../src/secondary.rs
    ../src/sparse_secondary.rs
    ../src/util.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../Cargo.lock
    ../LICENSE
    ../README.md
    ../RELEASES.md
    ../rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/version_check-0.9.5
)

LICENSE("Zlib")
VERSION(1.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/slotmap/slotmap-1.1.1.crate)

CRATE_SOURCES(
    ../src/basic.rs
    ../src/dense.rs
    ../src/hop.rs
    ../src/lib.rs
    ../src/secondary.rs
    ../src/sparse_secondary.rs
    ../src/util.rs
)

END()
