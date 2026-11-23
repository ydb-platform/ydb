# Native Rust build with dependencies shared through contrib/crates.

CRATE(thiserror)

CRATE_VERSION(2.0.21)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    build/probe.rs
    src/aserror.rs
    src/display.rs
    src/private.rs
    src/provide.rs
    src/var.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

CRATE_BUILD_SCRIPT(contrib/crates/thiserror-2.0.21/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(2.0.21)
ORIGINAL_SOURCE(https://static.crates.io/crates/thiserror/thiserror-2.0.21.crate)

CRATE_PROC_MACROS(
    contrib/crates/thiserror-impl-2.0.21
)

END()

RECURSE(build_script)
