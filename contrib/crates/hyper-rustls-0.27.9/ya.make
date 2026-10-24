# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(hyper_rustls)

CRATE_VERSION(0.27.9)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-ISC
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    http1
    tls12
)

PEERDIR(
    contrib/crates/http-1.4.2
    contrib/crates/hyper-1.11.0
    contrib/crates/hyper-util-0.1.20
    contrib/crates/rustls-0.23.42
    contrib/crates/tokio-1.53.1
    contrib/crates/tokio-rustls-0.26.4
    contrib/crates/tower-service-0.3.3
)

LICENSE("Apache-2.0 OR ISC OR MIT")
VERSION(0.27.9)
ORIGINAL_SOURCE(https://static.crates.io/crates/hyper-rustls/hyper-rustls-0.27.9.crate)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.33
    contrib/crates/tokio-macros-2.7.1
    contrib/crates/tracing-attributes-0.1.31
)

END()
