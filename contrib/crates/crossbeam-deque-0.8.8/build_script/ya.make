# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.8.8)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/deque.rs
    ../src/lib.rs
)

CRATE_INPUTS(
    ../CHANGELOG.md
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
VERSION(0.8.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/crossbeam-deque/crossbeam-deque-0.8.8.crate)

END()
