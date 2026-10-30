# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(opentelemetry_http)

CRATE_VERSION(0.32.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    reqwest
    reqwest-blocking
)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/http-1.5.0
    contrib/crates/opentelemetry-0.32.0
    contrib/crates/reqwest-0.13.4
)

LICENSE("Apache-2.0")
VERSION(0.32.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/opentelemetry-http/opentelemetry-http-0.32.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/async-trait-0.1.92
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/displaydoc-0.2.7
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
