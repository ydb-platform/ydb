# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(base64)

CRATE_VERSION(0.22.1)
CRATE_EDITION(2018)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    .circleci/config.yml
    .gitignore
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
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.22.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/base64/base64-0.22.1.crate)

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
    src/engine/tests.rs
    src/lib.rs
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

END()
