# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(flate2)

CRATE_VERSION(1.1.9)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bufreader.rs
    src/crc.rs
    src/deflate/bufread.rs
    src/deflate/mod.rs
    src/deflate/read.rs
    src/deflate/write.rs
    src/ffi/c.rs
    src/ffi/miniz_oxide.rs
    src/ffi/mod.rs
    src/ffi/zlib_rs.rs
    src/gz/bufread.rs
    src/gz/mod.rs
    src/gz/read.rs
    src/gz/write.rs
    src/mem.rs
    src/zio.rs
    src/zlib/bufread.rs
    src/zlib/mod.rs
    src/zlib/read.rs
    src/zlib/write.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    MAINTENANCE.md
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    any_impl
    any_zlib
    default
    miniz_oxide
    rust_backend
    zlib-rs
)

PEERDIR(
    contrib/crates/crc32fast-1.5.0
    contrib/crates/miniz_oxide-0.8.9
    contrib/crates/zlib-rs-0.6.6
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.9)
ORIGINAL_SOURCE(https://static.crates.io/crates/flate2/flate2-1.1.9.crate)

END()
