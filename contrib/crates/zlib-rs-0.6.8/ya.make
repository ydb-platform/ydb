# Native Rust build with dependencies shared through contrib/crates.

CRATE(zlib_rs)

CRATE_VERSION(0.6.8)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/adler32.rs
    src/adler32/avx2.rs
    src/adler32/avx512.rs
    src/adler32/avx512_vnni.rs
    src/adler32/generic.rs
    src/adler32/lsx.rs
    src/adler32/neon.rs
    src/adler32/wasm.rs
    src/allocate.rs
    src/c_api.rs
    src/cpu_features.rs
    src/crc32.rs
    src/crc32/acle.rs
    src/crc32/braid.rs
    src/crc32/combine.rs
    src/crc32/loongarch.rs
    src/crc32/pclmulqdq.rs
    src/crc32/vpclmulqdq.rs
    src/deflate.rs
    src/deflate/algorithm/fast.rs
    src/deflate/algorithm/huff.rs
    src/deflate/algorithm/medium.rs
    src/deflate/algorithm/mod.rs
    src/deflate/algorithm/quick.rs
    src/deflate/algorithm/rle.rs
    src/deflate/algorithm/slow.rs
    src/deflate/algorithm/stored.rs
    src/deflate/compare256.rs
    src/deflate/hash_calc.rs
    src/deflate/longest_match.rs
    src/deflate/pending.rs
    src/deflate/slide_hash.rs
    src/deflate/sym_buf.rs
    src/deflate/trees_tbl.rs
    src/deflate/window.rs
    src/inflate.rs
    src/inflate/bitreader.rs
    src/inflate/infback.rs
    src/inflate/inffixed_tbl.rs
    src/inflate/inftrees.rs
    src/inflate/window.rs
    src/inflate/writer.rs
    src/stable.rs
    src/weak_slice.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    src/deflate/test-data/inflate_buf_error.dat
    src/deflate/test-data/paper-100k.pdf
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    rust-allocator
    std
)

LICENSE("Zlib")
VERSION(0.6.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/zlib-rs/zlib-rs-0.6.8.crate)

END()
