# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(png)

CRATE_VERSION(0.18.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.18.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/change-png-info.rs
    examples/corpus-bench.rs
    examples/png-generate.rs
    examples/pngcheck.rs
    src/adam7.rs
    src/benchable_apis.rs
    src/chunk.rs
    src/common.rs
    src/decoder/interlace_info.rs
    src/decoder/mod.rs
    src/decoder/read_decoder.rs
    src/decoder/stream.rs
    src/decoder/transform/palette.rs
    src/decoder/transform.rs
    src/decoder/unfiltering_buffer.rs
    src/decoder/zlib.rs
    src/encoder.rs
    src/filter.rs
    src/srgb.rs
    src/test_utils.rs
    src/text_metadata.rs
    src/traits.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGES.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bitflags-2.13.2
    contrib/crates/crc32fast-1.5.3
    contrib/crates/fdeflate-0.3.7
    contrib/crates/flate2-1.1.10
    contrib/crates/miniz_oxide-0.8.9
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
