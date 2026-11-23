# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(bytes)

CRATE_VERSION(1.12.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/buf/buf_impl.rs
    src/buf/buf_mut.rs
    src/buf/chain.rs
    src/buf/iter.rs
    src/buf/limit.rs
    src/buf/mod.rs
    src/buf/reader.rs
    src/buf/take.rs
    src/buf/uninit_slice.rs
    src/buf/vec_deque.rs
    src/buf/writer.rs
    src/bytes.rs
    src/bytes_mut.rs
    src/fmt/debug.rs
    src/fmt/hex.rs
    src/fmt/mod.rs
    src/loom.rs
    src/serde.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    SECURITY.md
    clippy.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT")
VERSION(1.12.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/bytes/bytes-1.12.1.crate)

CRATE_FEATURES(
    serde
)

PEERDIR(
    contrib/crates/serde-1.0.229
)

CRATE_SOURCES(
    src/buf/buf_impl.rs
    src/buf/buf_mut.rs
    src/buf/chain.rs
    src/buf/iter.rs
    src/buf/limit.rs
    src/buf/mod.rs
    src/buf/reader.rs
    src/buf/take.rs
    src/buf/uninit_slice.rs
    src/buf/vec_deque.rs
    src/buf/writer.rs
    src/bytes.rs
    src/bytes_mut.rs
    src/fmt/debug.rs
    src/fmt/hex.rs
    src/fmt/mod.rs
    src/lib.rs
    src/loom.rs
    src/serde.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
