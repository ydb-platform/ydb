# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.0.229)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/crate_root.rs
    ../src/de/ignored_any.rs
    ../src/de/impls.rs
    ../src/de/mod.rs
    ../src/de/value.rs
    ../src/format.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/private/content.rs
    ../src/private/doc.rs
    ../src/private/mod.rs
    ../src/private/seed.rs
    ../src/private/size_hint.rs
    ../src/private/string.rs
    ../src/ser/fmt.rs
    ../src/ser/impls.rs
    ../src/ser/impossible.rs
    ../src/ser/mod.rs
    ../src/std_error.rs
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
    alloc
    default
    result
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.229)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde_core/serde_core-1.0.229.crate)

CRATE_FEATURES(
    rc
)

CRATE_SOURCES(
    ../src/crate_root.rs
    ../src/de/ignored_any.rs
    ../src/de/impls.rs
    ../src/de/mod.rs
    ../src/de/value.rs
    ../src/format.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/private/content.rs
    ../src/private/doc.rs
    ../src/private/mod.rs
    ../src/private/seed.rs
    ../src/private/size_hint.rs
    ../src/private/string.rs
    ../src/ser/fmt.rs
    ../src/ser/impls.rs
    ../src/ser/impossible.rs
    ../src/ser/mod.rs
    ../src/std_error.rs
)

END()
