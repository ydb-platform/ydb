# Native Rust build with dependencies shared through contrib/crates.

CRATE(tungstenite)

CRATE_VERSION(0.29.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/buffer.rs
    src/client.rs
    src/error.rs
    src/handshake/client.rs
    src/handshake/headers.rs
    src/handshake/machine.rs
    src/handshake/mod.rs
    src/handshake/server.rs
    src/protocol/frame/coding.rs
    src/protocol/frame/frame.rs
    src/protocol/frame/mask.rs
    src/protocol/frame/mod.rs
    src/protocol/frame/utf8.rs
    src/protocol/message.rs
    src/protocol/mod.rs
    src/server.rs
    src/stream.rs
    src/tls.rs
    src/utf8.rs
    src/util.rs
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
    data-encoding
    handshake
    http
    httparse
    sha1
)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/data-encoding-2.11.1
    contrib/crates/http-1.5.0
    contrib/crates/httparse-1.10.1
    contrib/crates/log-0.4.34
    contrib/crates/rand-0.9.5
    contrib/crates/sha1-0.10.7
    contrib/crates/thiserror-2.0.21
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/tungstenite/tungstenite-0.29.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/zerocopy-derive-0.8.63
)

END()
