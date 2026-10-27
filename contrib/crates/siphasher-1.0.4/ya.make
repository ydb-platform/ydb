# Native Rust build with dependencies shared through contrib/crates.

CRATE(siphasher)

CRATE_VERSION(1.0.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/common.rs
    src/sip.rs
    src/sip128.rs
    src/tests.rs
    src/tests128.rs
)

CRATE_INPUTS(
    COPYING
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/siphasher/siphasher-1.0.4.crate)

END()
