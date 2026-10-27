# Native Rust build with dependencies shared through contrib/crates.

CRATE(litemap)

CRATE_VERSION(0.8.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/databake.rs
    src/map.rs
    src/serde.rs
    src/serde_helpers.rs
    src/store/mod.rs
    src/store/slice_impl.rs
    src/store/vec_impl.rs
    src/testing.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Unicode-3.0")
VERSION(0.8.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/litemap/litemap-0.8.3.crate)

CRATE_FEATURES(
    alloc
)

END()
