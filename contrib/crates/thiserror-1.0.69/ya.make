# Native Rust build with dependencies shared through contrib/crates.

CRATE(thiserror)

CRATE_VERSION(1.0.69)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    build/probe.rs
    src/aserror.rs
    src/display.rs
    src/provide.rs
)

CRATE_INPUTS(
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/thiserror-1.0.69/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.69)
ORIGINAL_SOURCE(https://static.crates.io/crates/thiserror/thiserror-1.0.69.crate)

CRATE_PROC_MACROS(
    contrib/crates/thiserror-impl-1.0.69
)

END()

RECURSE(build_script)
