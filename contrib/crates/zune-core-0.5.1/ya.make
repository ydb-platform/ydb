# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(zune_core)

CRATE_VERSION(0.5.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.5.1)
LICENSE("MIT OR Apache-2.0 OR Zlib")

CRATE_SOURCES(
    src/bit_depth.rs
    src/bytestream/reader/no_std_readers.rs
    src/bytestream/reader/std_readers.rs
    src/bytestream/reader/zcursor_no_std.rs
    src/bytestream/reader.rs
    src/bytestream/traits.rs
    src/bytestream/writer/no_std_writer.rs
    src/bytestream/writer/std_writer.rs
    src/bytestream/writer.rs
    src/bytestream.rs
    src/colorspace.rs
    src/log.rs
    src/options/decoder.rs
    src/options/encoder.rs
    src/options.rs
    src/result.rs
    src/serde.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-ZLIB
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(std)

END()
