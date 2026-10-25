# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.0.104)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/backtrace.rs
    ../src/chain.rs
    ../src/context.rs
    ../src/ensure.rs
    ../src/error.rs
    ../src/fmt.rs
    ../src/kind.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/nightly.rs
    ../src/ptr.rs
    ../src/wrapper.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitignore
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
VERSION(1.0.104)
ORIGINAL_SOURCE(https://static.crates.io/crates/anyhow/anyhow-1.0.104.crate)

CRATE_SOURCES(
    ../src/backtrace.rs
    ../src/chain.rs
    ../src/context.rs
    ../src/ensure.rs
    ../src/error.rs
    ../src/fmt.rs
    ../src/kind.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/nightly.rs
    ../src/ptr.rs
    ../src/wrapper.rs
)

END()
