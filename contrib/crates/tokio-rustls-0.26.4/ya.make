# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tokio_rustls)

CRATE_VERSION(0.26.4)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
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
    contrib/crates/rustls-0.23.42
    contrib/crates/tokio-1.53.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.26.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/tokio-rustls/tokio-rustls-0.26.4.crate)

CRATE_PROC_MACROS(
    contrib/crates/tokio-macros-2.7.1
)

END()
