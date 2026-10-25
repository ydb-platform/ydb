# Native Rust build with dependencies shared through contrib/crates.

CRATE(rand_chacha)

CRATE_VERSION(0.3.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/chacha.rs
    src/guts.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    COPYRIGHT
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/ppv-lite86-0.2.21
    contrib/crates/rand_core-0.6.4
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand_chacha/rand_chacha-0.3.1.crate)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
