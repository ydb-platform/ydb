# Native Rust build with dependencies shared through contrib/crates.

CRATE(log)

CRATE_VERSION(0.4.34)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/__private_api.rs
    src/kv/error.rs
    src/kv/key.rs
    src/kv/mod.rs
    src/kv/source.rs
    src/kv/value.rs
    src/macros.rs
    src/serde.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    triagebot.toml
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.34)
ORIGINAL_SOURCE(https://static.crates.io/crates/log/log-0.4.34.crate)

CRATE_FEATURES(
    kv
    kv_unstable
    release_max_level_debug
    std
    value-bag
)

CRATE_FEATURES(
    alloc
)

PEERDIR(
    contrib/crates/value-bag-1.14.1
)

END()
