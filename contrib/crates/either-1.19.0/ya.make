# Native Rust build with dependencies shared through contrib/crates.

CRATE(either)

CRATE_VERSION(1.19.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/into_either.rs
    src/iterator.rs
    src/serde_untagged.rs
    src/serde_untagged_optional.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
    use_std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.19.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/either/either-1.19.0.crate)

CRATE_FEATURES(
    default
    serde
)

PEERDIR(
    contrib/crates/serde-1.0.229
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
