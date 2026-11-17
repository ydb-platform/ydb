# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(fdeflate)

CRATE_VERSION(0.3.7)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.7)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/compress.rs
    src/decompress/tests/test_utils.rs
    src/decompress.rs
    src/huffman.rs
    src/tables.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/simd-adler32-0.3.10
)

CRATE_INPUTS(
    tests/input-chunking-sensitivity-example1.zz
    tests/input-chunking-sensitivity-example2.zz
    tests/input-chunking-sensitivity-example3.zz
)

CRATE_SOURCES(
    src/compress.rs
    src/decompress.rs
    src/decompress/tests/test_utils.rs
    src/huffman.rs
    src/lib.rs
    src/tables.rs
)

END()
