# Native Rust build with dependencies shared through contrib/crates.

CRATE(valuable_serde)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/serde-1.0.229
    contrib/crates/valuable-0.1.1
)

LICENSE("MIT")
VERSION(0.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/valuable-serde/valuable-serde-0.1.1.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/valuable-derive-0.1.1
)

END()
