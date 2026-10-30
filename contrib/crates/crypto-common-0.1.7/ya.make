# Native Rust build with dependencies shared through contrib/crates.

CRATE(crypto_common)

CRATE_VERSION(0.1.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/generic-array-0.14.7
    contrib/crates/typenum-1.20.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/crypto-common/crypto-common-0.1.7.crate)

END()
