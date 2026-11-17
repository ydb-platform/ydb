# Native Rust build with dependencies shared through contrib/crates.

CRATE(tokio_tungstenite)

CRATE_VERSION(0.29.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/compat.rs
    src/connect.rs
    src/handshake.rs
    src/stream.rs
    src/tls.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    connect
    default
    handshake
    stream
)

PEERDIR(
    contrib/crates/futures-util-0.3.34
    contrib/crates/log-0.4.34
    contrib/crates/tokio-1.54.1
    contrib/crates/tungstenite-0.29.0
)

LICENSE("MIT")
VERSION(0.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/tokio-tungstenite/tokio-tungstenite-0.29.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/zerocopy-derive-0.8.63
)

END()
