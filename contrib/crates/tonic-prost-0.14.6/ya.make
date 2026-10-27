# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tonic_prost)

CRATE_VERSION(0.14.6)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/codec.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/prost-0.14.4
    contrib/crates/tonic-0.14.6
)

LICENSE("MIT")
VERSION(0.14.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/tonic-prost/tonic-prost-0.14.6.crate)

CRATE_SOURCES(
    src/codec.rs
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/async-trait-0.1.92
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/futures-macro-0.3.34
    contrib/crates/pin-project-internal-1.1.13
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
    contrib/crates/zerocopy-derive-0.8.63
)

END()
