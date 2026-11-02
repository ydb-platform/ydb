# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(image_webp)

CRATE_VERSION(0.2.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.2.4)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/alpha_blending.rs
    src/decoder.rs
    src/encoder.rs
    src/extended.rs
    src/huffman.rs
    src/loop_filter.rs
    src/lossless.rs
    src/lossless_transform.rs
    src/mod.rs
    src/transform.rs
    src/vp8.rs
    src/vp8_arithmetic_decoder.rs
    src/yuv.rs
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
    contrib/crates/byteorder-lite-0.1.0
    contrib/crates/quick-error-2.0.1
)

END()
