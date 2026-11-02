# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zstd)

CRATE_VERSION(0.13.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bulk/compressor.rs
    src/bulk/decompressor.rs
    src/bulk/mod.rs
    src/bulk/tests.rs
    src/dict.rs
    src/stream/functions.rs
    src/stream/mod.rs
    src/stream/raw.rs
    src/stream/read/mod.rs
    src/stream/read/tests.rs
    src/stream/tests.rs
    src/stream/write/mod.rs
    src/stream/write/tests.rs
    src/stream/zio/mod.rs
    src/stream/zio/reader.rs
    src/stream/zio/writer.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitattributes
    .gitignore
    .gitmodules
    Cargo.lock
    LICENSE
    Readme.md
    assets/example.txt
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/zstd-safe-7.3.0
)

LICENSE("MIT")
VERSION(0.13.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/zstd/zstd-0.13.3.crate)

CRATE_FEATURES(
    arrays
    default
    experimental
    legacy
    zdict_builder
    zstdmt
)

CRATE_INPUTS(
    assets/example.txt
)

CRATE_SOURCES(
    src/bulk/compressor.rs
    src/bulk/decompressor.rs
    src/bulk/mod.rs
    src/bulk/tests.rs
    src/dict.rs
    src/lib.rs
    src/stream/functions.rs
    src/stream/mod.rs
    src/stream/raw.rs
    src/stream/read/mod.rs
    src/stream/read/tests.rs
    src/stream/tests.rs
    src/stream/write/mod.rs
    src/stream/write/tests.rs
    src/stream/zio/mod.rs
    src/stream/zio/reader.rs
    src/stream/zio/writer.rs
)

END()
