# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(bon)

CRATE_VERSION(3.9.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/__/better_errors.rs
    src/__/cfg_eval.rs
    src/__/ide.rs
    src/__/mod.rs
    src/builder_state.rs
    src/collections.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    implied-bounds
)

LICENSE("MIT OR Apache-2.0")
VERSION(3.9.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/bon/bon-3.9.3.crate)

CRATE_FEATURES(
    default
    std
)

CRATE_PROC_MACROS(
    contrib/crates/bon-macros-3.9.3
    contrib/crates/darling_macro-0.23.0
    contrib/crates/rustversion-1.0.23
)

END()
