# Native Rust build with dependencies shared through contrib/crates.

CRATE(tokio_rustls)

CRATE_VERSION(0.26.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/client.rs
    src/common/handshake.rs
    src/common/mod.rs
    src/common/test_stream.rs
    src/server.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    logging
    ring
    tls12
)

PEERDIR(
    contrib/crates/rustls-0.23.46
    contrib/crates/tokio-1.54.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.26.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/tokio-rustls/tokio-rustls-0.26.6.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
)

END()
