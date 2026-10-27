# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(anstyle)

CRATE_VERSION(1.0.14)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/color.rs
    src/effect.rs
    src/macros.rs
    src/reset.rs
    src/style.rs
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
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.14)
ORIGINAL_SOURCE(https://static.crates.io/crates/anstyle/anstyle-1.0.14.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/color.rs
    src/effect.rs
    src/lib.rs
    src/macros.rs
    src/reset.rs
    src/style.rs
)

END()
