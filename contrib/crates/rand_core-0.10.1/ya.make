# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rand_core)

CRATE_VERSION(0.10.1)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/block.rs
    src/seedable_rng.rs
    src/unwrap_err.rs
    src/utils.rs
    src/word.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    COPYRIGHT
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand_core/rand_core-0.10.1.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/block.rs
    src/lib.rs
    src/seedable_rng.rs
    src/unwrap_err.rs
    src/utils.rs
    src/word.rs
)

END()
