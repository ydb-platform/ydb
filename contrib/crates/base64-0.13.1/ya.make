# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(base64)

CRATE_VERSION(0.13.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    examples/base64.rs
    examples/make_tables.rs
    src/chunked_encoder.rs
    src/decode.rs
    src/display.rs
    src/encode.rs
    src/read/decoder.rs
    src/read/decoder_tests.rs
    src/read/mod.rs
    src/tables.rs
    src/tests.rs
    src/write/encoder.rs
    src/write/encoder_string_writer.rs
    src/write/encoder_tests.rs
    src/write/mod.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .travis.yml
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASE-NOTES.md
    icon_CLion.svg
)

CRATE_FEATURES(
    default
    std
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.13.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/base64/base64-0.13.1.crate)

CRATE_SOURCES(
    src/chunked_encoder.rs
    src/decode.rs
    src/display.rs
    src/encode.rs
    src/lib.rs
    src/read/decoder.rs
    src/read/decoder_tests.rs
    src/read/mod.rs
    src/tables.rs
    src/tests.rs
    src/write/encoder.rs
    src/write/encoder_string_writer.rs
    src/write/encoder_tests.rs
    src/write/mod.rs
)

END()
