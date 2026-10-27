# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(erased_serde)

CRATE_VERSION(0.4.10)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/any.rs
    src/de.rs
    src/error.rs
    src/features_check/error.rs
    src/features_check/mod.rs
    src/macros.rs
    src/map.rs
    src/private.rs
    src/sealed.rs
    src/ser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

PEERDIR(
    contrib/crates/serde_core-1.0.229
    contrib/crates/typeid-1.0.3
)

CRATE_BUILD_SCRIPT(contrib/crates/erased-serde-0.4.10/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.10)
ORIGINAL_SOURCE(https://static.crates.io/crates/erased-serde/erased-serde-0.4.10.crate)

CRATE_SOURCES(
    src/private.rs
)

CRATE_SOURCES(
    src/any.rs
    src/de.rs
    src/error.rs
    src/features_check/error.rs
    src/features_check/mod.rs
    src/lib.rs
    src/macros.rs
    src/map.rs
    src/sealed.rs
    src/ser.rs
)

END()

RECURSE(build_script)
