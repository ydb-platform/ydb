# Native Rust build with dependencies shared through contrib/crates.

CRATE(block_buffer)

CRATE_VERSION(0.10.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/sealed.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/generic-array-0.14.7
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/block-buffer/block-buffer-0.10.4.crate)

END()
