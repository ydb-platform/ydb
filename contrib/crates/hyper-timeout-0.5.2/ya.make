# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(hyper_timeout)

CRATE_VERSION(0.5.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/stream.rs
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

PEERDIR(
    contrib/crates/hyper-1.12.0
    contrib/crates/hyper-util-0.1.21
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/tokio-1.54.1
    contrib/crates/tower-service-0.3.3
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.5.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/hyper-timeout/hyper-timeout-0.5.2.crate)

CRATE_SOURCES(
    src/lib.rs
    src/stream.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
)

END()
