# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(multimap)

CRATE_VERSION(0.10.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/entry.rs
    src/serde.rs
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

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/multimap/multimap-0.10.1.crate)

CRATE_SOURCES(
    src/entry.rs
    src/lib.rs
    src/serde.rs
)

END()
