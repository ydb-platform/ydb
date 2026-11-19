# Native Rust build with dependencies shared through contrib/crates.

CRATE(hyper_rustls)

CRATE_VERSION(0.27.10)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/config.rs
    src/connector.rs
    src/connector/builder.rs
    src/stream.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-ISC
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    http1
    native-tokio
    ring
    rustls-native-certs
    tls12
)

PEERDIR(
    contrib/crates/http-1.5.0
    contrib/crates/hyper-1.12.0
    contrib/crates/hyper-util-0.1.21
    contrib/crates/rustls-0.23.46
    contrib/crates/rustls-native-certs-0.8.5
    contrib/crates/tokio-1.54.1
    contrib/crates/tokio-rustls-0.26.6
    contrib/crates/tower-service-0.3.3
)

LICENSE("Apache-2.0 OR ISC OR MIT")
VERSION(0.27.10)
ORIGINAL_SOURCE(https://static.crates.io/crates/hyper-rustls/hyper-rustls-0.27.10.crate)

CRATE_FEATURES(
    http2
    webpki-roots
    webpki-tokio
)

PEERDIR(
    contrib/crates/webpki-roots-1.0.11
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
)

END()
