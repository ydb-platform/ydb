# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.5.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(1.5.0)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/baseline.rs
    ../src/combine.rs
    ../src/lib.rs
    ../src/specialized/aarch64.rs
    ../src/specialized/mod.rs
    ../src/specialized/pclmulqdq.rs
    ../src/table.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)

END()
