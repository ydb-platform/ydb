# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(siphasher)

CRATE_VERSION(1.0.3)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    COPYING
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/siphasher/siphasher-1.0.3.crate)

END()
