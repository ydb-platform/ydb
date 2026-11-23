# Native Rust build with dependencies shared through contrib/crates.

CRATE(toml_writer)

CRATE_VERSION(1.1.3+spec-1.1.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/integer.rs
    src/key.rs
    src/string.rs
    src/value.rs
    src/write.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.3+spec-1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/toml_writer/toml_writer-1.1.3+spec-1.1.0.crate)

END()
