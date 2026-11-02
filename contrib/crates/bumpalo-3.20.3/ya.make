# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(bumpalo)

CRATE_VERSION(3.20.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alloc.rs
    src/boxed.rs
    src/collections/collect_in.rs
    src/collections/mod.rs
    src/collections/raw_vec.rs
    src/collections/str/lossy.rs
    src/collections/str/mod.rs
    src/collections/string.rs
    src/collections/vec.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

LICENSE("MIT OR Apache-2.0")
VERSION(3.20.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/bumpalo/bumpalo-3.20.3.crate)

CRATE_FEATURES(
    allocator-api2
)

PEERDIR(
    contrib/crates/allocator-api2-0.2.21
)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/alloc.rs
    src/boxed.rs
    src/collections/collect_in.rs
    src/collections/mod.rs
    src/collections/raw_vec.rs
    src/collections/str/lossy.rs
    src/collections/str/mod.rs
    src/collections/string.rs
    src/collections/vec.rs
    src/lib.rs
)

END()
