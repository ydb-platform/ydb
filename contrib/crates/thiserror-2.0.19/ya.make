# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(thiserror)

CRATE_VERSION(2.0.19)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
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

CRATE_BUILD_SCRIPT(contrib/crates/thiserror-2.0.19/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(2.0.19)
ORIGINAL_SOURCE(https://static.crates.io/crates/thiserror/thiserror-2.0.19.crate)

CRATE_PROC_MACROS(
    contrib/crates/thiserror-impl-2.0.19
)

END()

RECURSE(build_script)
