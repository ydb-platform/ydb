# Native Rust build with dependencies shared through contrib/crates.

CRATE(async_compression)

CRATE_VERSION(0.4.51)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/futures/bufread/generic/decoder.rs
    src/futures/bufread/generic/encoder.rs
    src/futures/bufread/generic/mod.rs
    src/futures/bufread/macros/decoder.rs
    src/futures/bufread/macros/encoder.rs
    src/futures/bufread/macros/mod.rs
    src/futures/bufread/mod.rs
    src/futures/mod.rs
    src/futures/write/buf_writer.rs
    src/futures/write/generic/decoder.rs
    src/futures/write/generic/encoder.rs
    src/futures/write/generic/mod.rs
    src/futures/write/macros/decoder.rs
    src/futures/write/macros/encoder.rs
    src/futures/write/macros/mod.rs
    src/futures/write/mod.rs
    src/generic/bufread/decoder.rs
    src/generic/bufread/encoder.rs
    src/generic/bufread/mod.rs
    src/generic/mod.rs
    src/generic/write/buf_write.rs
    src/generic/write/buf_writer.rs
    src/generic/write/decoder.rs
    src/generic/write/encoder.rs
    src/generic/write/mod.rs
    src/macros.rs
    src/tokio/bufread/generic/decoder.rs
    src/tokio/bufread/generic/encoder.rs
    src/tokio/bufread/generic/mod.rs
    src/tokio/bufread/macros/decoder.rs
    src/tokio/bufread/macros/encoder.rs
    src/tokio/bufread/macros/mod.rs
    src/tokio/bufread/mod.rs
    src/tokio/mod.rs
    src/tokio/write/buf_writer.rs
    src/tokio/write/generic/decoder.rs
    src/tokio/write/generic/encoder.rs
    src/tokio/write/generic/mod.rs
    src/tokio/write/macros/decoder.rs
    src/tokio/write/macros/encoder.rs
    src/tokio/write/macros/mod.rs
    src/tokio/write/mod.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    gzip
    tokio
)

PEERDIR(
    contrib/crates/compression-codecs-0.4.46
    contrib/crates/compression-core-0.4.34
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/tokio-1.54.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.51)
ORIGINAL_SOURCE(https://static.crates.io/crates/async-compression/async-compression-0.4.51.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
)

END()
