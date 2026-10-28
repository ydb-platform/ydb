# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rand_chacha)

CRATE_VERSION(0.9.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/chacha.rs
    src/guts.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    COPYRIGHT
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
    contrib/crates/ppv-lite86-0.2.21
    contrib/crates/rand_core-0.9.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.9.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand_chacha/rand_chacha-0.9.0.crate)

CRATE_SOURCES(
    src/chacha.rs
    src/guts.rs
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
