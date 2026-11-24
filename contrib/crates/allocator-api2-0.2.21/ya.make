# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(allocator_api2)

CRATE_VERSION(0.2.21)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/nightly.rs
    src/stable/alloc/global.rs
    src/stable/alloc/mod.rs
    src/stable/alloc/system.rs
    src/stable/boxed.rs
    src/stable/macros.rs
    src/stable/mod.rs
    src/stable/raw_vec.rs
    src/stable/slice.rs
    src/stable/unique.rs
    src/stable/vec/drain.rs
    src/stable/vec/into_iter.rs
    src/stable/vec/mod.rs
    src/stable/vec/partial_eq.rs
    src/stable/vec/set_len_on_drop.rs
    src/stable/vec/splice.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.21)
ORIGINAL_SOURCE(https://static.crates.io/crates/allocator-api2/allocator-api2-0.2.21.crate)

CRATE_SOURCES(
    src/lib.rs
    src/nightly.rs
    src/stable/alloc/global.rs
    src/stable/alloc/mod.rs
    src/stable/alloc/system.rs
    src/stable/boxed.rs
    src/stable/macros.rs
    src/stable/mod.rs
    src/stable/raw_vec.rs
    src/stable/slice.rs
    src/stable/unique.rs
    src/stable/vec/drain.rs
    src/stable/vec/into_iter.rs
    src/stable/vec/mod.rs
    src/stable/vec/partial_eq.rs
    src/stable/vec/set_len_on_drop.rs
    src/stable/vec/splice.rs
)

END()
