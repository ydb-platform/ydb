# Native Rust build with dependencies shared through contrib/crates.

CRATE(serde_spanned)

CRATE_VERSION(1.1.2)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/de.rs
    src/spanned.rs
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
    serde
    std
)

PEERDIR(
    contrib/crates/serde_core-1.0.229
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde_spanned/serde_spanned-1.1.2.crate)

END()
