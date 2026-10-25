# Native Rust build with dependencies shared through contrib/crates.

CRATE(base64)

CRATE_VERSION(0.23.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alphabet.rs
    src/chunked_encoder.rs
    src/decode.rs
    src/display.rs
    src/encode.rs
    src/engine/general_purpose/decode.rs
    src/engine/general_purpose/decode_suffix.rs
    src/engine/general_purpose/mod.rs
    src/engine/mod.rs
    src/engine/naive.rs
    src/engine/simd.rs
    src/engine/tests.rs
    src/prelude.rs
    src/read/decoder.rs
    src/read/decoder_tests.rs
    src/read/mod.rs
    src/tests.rs
    src/write/encoder.rs
    src/write/encoder_string_writer.rs
    src/write/encoder_tests.rs
    src/write/mod.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASE-NOTES.md
    clippy.toml
    icon_CLion.svg
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    simd-unsafe
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.23.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/base64/base64-0.23.1.crate)

END()
