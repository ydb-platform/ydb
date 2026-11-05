# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.8.0)
CRATE_EDITION(2015)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/offset_of.rs
    ../src/raw_field.rs
    ../src/span_of.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
    ../LICENSE
    ../README.md
)

CRATE_FEATURES(
    default
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/autocfg-1.5.1
)

LICENSE("MIT")
VERSION(0.8.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/memoffset/memoffset-0.8.0.crate)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/offset_of.rs
    ../src/raw_field.rs
    ../src/span_of.rs
)

END()
