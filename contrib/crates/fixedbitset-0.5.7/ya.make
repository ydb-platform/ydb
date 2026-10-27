# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(fixedbitset)

CRATE_VERSION(0.5.7)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/block/avx.rs
    src/block/avx2.rs
    src/block/default.rs
    src/block/mod.rs
    src/block/sse2.rs
    src/block/wasm.rs
    src/range.rs
    src/serde_impl.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.5.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/fixedbitset/fixedbitset-0.5.7.crate)

CRATE_SOURCES(
    src/block/avx.rs
    src/block/avx2.rs
    src/block/default.rs
    src/block/mod.rs
    src/block/sse2.rs
    src/block/wasm.rs
    src/lib.rs
    src/range.rs
    src/serde_impl.rs
)

END()
