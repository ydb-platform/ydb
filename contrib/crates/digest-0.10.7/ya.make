# Native Rust build with dependencies shared through contrib/crates.

CRATE(digest)

CRATE_VERSION(0.10.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/core_api.rs
    src/core_api/ct_variable.rs
    src/core_api/rt_variable.rs
    src/core_api/wrapper.rs
    src/core_api/xof_reader.rs
    src/dev.rs
    src/dev/fixed.rs
    src/dev/mac.rs
    src/dev/rng.rs
    src/dev/variable.rs
    src/dev/xof.rs
    src/digest.rs
    src/mac.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    block-buffer
    core-api
    default
    mac
    std
    subtle
)

PEERDIR(
    contrib/crates/block-buffer-0.10.4
    contrib/crates/crypto-common-0.1.7
    contrib/crates/subtle-2.6.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/digest/digest-0.10.7.crate)

END()
