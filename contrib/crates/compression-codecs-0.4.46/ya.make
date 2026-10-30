# Native Rust build with dependencies shared through contrib/crates.

CRATE(compression_codecs)

CRATE_VERSION(0.4.46)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/brotli/decoder.rs
    src/brotli/encoder.rs
    src/brotli/mod.rs
    src/brotli/params.rs
    src/bzip2/decoder.rs
    src/bzip2/encoder.rs
    src/bzip2/mod.rs
    src/bzip2/params.rs
    src/deflate/decoder.rs
    src/deflate/encoder.rs
    src/deflate/mod.rs
    src/deflate64/decoder.rs
    src/deflate64/mod.rs
    src/flate/decoder.rs
    src/flate/encoder.rs
    src/flate/mod.rs
    src/flate/params.rs
    src/gzip/decoder.rs
    src/gzip/encoder.rs
    src/gzip/header.rs
    src/gzip/mod.rs
    src/lz4/decoder.rs
    src/lz4/encoder.rs
    src/lz4/mod.rs
    src/lz4/params.rs
    src/lzma/decoder.rs
    src/lzma/encoder.rs
    src/lzma/mod.rs
    src/lzma/params.rs
    src/mbrotli/decoder.rs
    src/mbrotli/encoder.rs
    src/mbrotli/mod.rs
    src/mbrotli/params.rs
    src/mzstd/decoder.rs
    src/mzstd/encoder.rs
    src/mzstd/mod.rs
    src/mzstd/params.rs
    src/xz/decoder.rs
    src/xz/encoder.rs
    src/xz/mod.rs
    src/xz2/decoder.rs
    src/xz2/encoder.rs
    src/xz2/mod.rs
    src/zlib/decoder.rs
    src/zlib/encoder.rs
    src/zlib/mod.rs
    src/zstd/decoder.rs
    src/zstd/encoder.rs
    src/zstd/mod.rs
    src/zstd/params.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    flate2
    gzip
    memchr
)

PEERDIR(
    contrib/crates/compression-core-0.4.34
    contrib/crates/flate2-1.1.10
    contrib/crates/memchr-2.8.3
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.46)
ORIGINAL_SOURCE(https://static.crates.io/crates/compression-codecs/compression-codecs-0.4.46.crate)

END()
