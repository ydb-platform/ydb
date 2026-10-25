# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(litemap)

CRATE_VERSION(0.8.2)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Unicode-3.0")
VERSION(0.8.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/litemap/litemap-0.8.2.crate)

END()
