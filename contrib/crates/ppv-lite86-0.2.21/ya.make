# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(ppv_lite86)

CRATE_VERSION(0.2.21)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/generic.rs
    src/soft.rs
    src/types.rs
    src/x86_64/mod.rs
    src/x86_64/sse2.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    simd
    std
)

PEERDIR(
    contrib/crates/zerocopy-0.8.63
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.21)
ORIGINAL_SOURCE(https://static.crates.io/crates/ppv-lite86/ppv-lite86-0.2.21.crate)

CRATE_SOURCES(
    src/generic.rs
    src/lib.rs
    src/soft.rs
    src/types.rs
    src/x86_64/mod.rs
    src/x86_64/sse2.rs
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
