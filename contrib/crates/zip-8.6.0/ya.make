# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zip)

CRATE_VERSION(8.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/aes.rs
    src/aes_ctr.rs
    src/compression.rs
    src/cp437.rs
    src/crc32.rs
    src/datetime.rs
    src/extra_fields/aex_encryption.rs
    src/extra_fields/extended_timestamp.rs
    src/extra_fields/mod.rs
    src/extra_fields/ntfs.rs
    src/extra_fields/zip64_extended_information.rs
    src/extra_fields/zipinfo_utf8.rs
    src/legacy/huffman.rs
    src/legacy/implode.rs
    src/legacy/mod.rs
    src/legacy/reduce.rs
    src/legacy/shrink.rs
    src/path.rs
    src/read.rs
    src/read/config.rs
    src/read/magic_finder.rs
    src/read/readers.rs
    src/read/stream.rs
    src/read/zip_archive.rs
    src/result.rs
    src/spec.rs
    src/types.rs
    src/unstable.rs
    src/write.rs
    src/zipcrypto.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitattributes
    .gitignore
    .pre-commit-config.yaml
    .whitesource
    CHANGELOG.md
    CODE_OF_CONDUCT.md
    CONTRIBUTING.md
    Cargo.lock
    LICENSE
    README.md
    SECURITY.md
    cliff.toml
    release-plz.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    _bzip2_any
    _deflate-any
    bzip2
    deflate
    deflate-flate2
    deflate-flate2-zlib-rs
    deflate-zopfli
    zstd
)

PEERDIR(
    contrib/crates/bzip2-0.6.1
    contrib/crates/crc32fast-1.5.3
    contrib/crates/flate2-1.1.10
    contrib/crates/indexmap-2.14.2
    contrib/crates/memchr-2.8.3
    contrib/crates/typed-path-0.12.3
    contrib/crates/zopfli-0.8.4
    contrib/crates/zstd-0.13.3
)

LICENSE("MIT")
VERSION(8.6.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/zip/zip-8.6.0.crate)

END()
