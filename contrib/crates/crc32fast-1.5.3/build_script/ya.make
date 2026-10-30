# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.5.3)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
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

LICENSE("MIT OR Apache-2.0")
VERSION(1.5.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/crc32fast/crc32fast-1.5.3.crate)

END()
