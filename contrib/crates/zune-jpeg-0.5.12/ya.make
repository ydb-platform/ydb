# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(zune_jpeg)

CRATE_VERSION(0.5.12)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.5.12)
LICENSE("MIT OR Apache-2.0 OR Zlib")

CRATE_SOURCES(
    src/bitstream.rs
    src/color_convert/avx.rs
    src/color_convert/neon64.rs
    src/color_convert/scalar.rs
    src/color_convert.rs
    src/components.rs
    src/decoder.rs
    src/errors.rs
    src/headers.rs
    src/huffman.rs
    src/idct/avx2.rs
    src/idct/neon.rs
    src/idct/scalar.rs
    src/idct.rs
    src/marker.rs
    src/mcu.rs
    src/mcu_prog.rs
    src/misc.rs
    src/unsafe_utils.rs
    src/unsafe_utils_avx2.rs
    src/unsafe_utils_neon.rs
    src/upsampler/avx2.rs
    src/upsampler/neon.rs
    src/upsampler/portable_simd.rs
    src/upsampler/scalar.rs
    src/upsampler.rs
    src/worker.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Benches.md
    Cargo.lock
    Changelog.md
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-ZLIB
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    neon
    std
    x86
)
PEERDIR(
    contrib/crates/zune-core-0.5.1
)

END()
