# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(2.0.21)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../build/probe.rs
    ../src/aserror.rs
    ../src/display.rs
    ../src/lib.rs
    ../src/private.rs
    ../src/provide.rs
    ../src/var.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(2.0.21)
ORIGINAL_SOURCE(https://static.crates.io/crates/thiserror/thiserror-2.0.21.crate)

END()
