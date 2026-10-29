# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.0.69)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../build/probe.rs
    ../src/aserror.rs
    ../src/display.rs
    ../src/lib.rs
    ../src/provide.rs
)

CRATE_INPUTS(
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.69)
ORIGINAL_SOURCE(https://static.crates.io/crates/thiserror/thiserror-1.0.69.crate)

END()
