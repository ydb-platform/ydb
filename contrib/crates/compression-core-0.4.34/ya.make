# Native Rust build with dependencies shared through contrib/crates.

CRATE(compression_core)

CRATE_VERSION(0.4.34)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/level.rs
    src/unshared.rs
    src/util.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.34)
ORIGINAL_SOURCE(https://static.crates.io/crates/compression-core/compression-core-0.4.34.crate)

END()
