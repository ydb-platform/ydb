# Native Rust build with dependencies shared through contrib/crates.

CRATE(futures_io)

CRATE_VERSION(0.3.34)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.34)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-io/futures-io-0.3.34.crate)

CRATE_FEATURES(
    default
)

END()
