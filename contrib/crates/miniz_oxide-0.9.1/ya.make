# Native Rust build with dependencies shared through contrib/crates.

CRATE(miniz_oxide)

CRATE_VERSION(0.9.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/deflate/buffer.rs
    src/deflate/core.rs
    src/deflate/mod.rs
    src/deflate/stored.rs
    src/deflate/stream.rs
    src/deflate/zlib.rs
    src/inflate/core.rs
    src/inflate/mod.rs
    src/inflate/output_buffer.rs
    src/inflate/stream.rs
    src/serde/big_array.rs
    src/serde/mod.rs
    src/shared.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    LICENSE-APACHE.md
    LICENSE-MIT.md
    LICENSE-ZLIB.md
    Readme.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    simd
    simd-adler32
    with-alloc
)

PEERDIR(
    contrib/crates/adler2-2.0.1
    contrib/crates/simd-adler32-0.3.10
)

LICENSE("MIT OR Zlib OR Apache-2.0")
VERSION(0.9.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/miniz_oxide/miniz_oxide-0.9.1.crate)

END()
