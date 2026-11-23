# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(weezl)

CRATE_VERSION(0.1.12)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.12)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    bin/lzw.rs
    examples/lzw-compress.rs
    examples/lzw-decompress.rs
    src/decode.rs
    src/decode_into_async.rs
    src/encode.rs
    src/encode_into_async.rs
    src/error.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    Changes.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    alloc
    default
    std
)

END()
