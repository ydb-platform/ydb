# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(axum_core)

CRATE_VERSION(0.5.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/body.rs
    src/error.rs
    src/ext_traits/mod.rs
    src/ext_traits/request.rs
    src/ext_traits/request_parts.rs
    src/extract/default_body_limit.rs
    src/extract/from_ref.rs
    src/extract/mod.rs
    src/extract/option.rs
    src/extract/rejection.rs
    src/extract/request_parts.rs
    src/extract/tuple.rs
    src/macros.rs
    src/response/append_headers.rs
    src/response/into_response.rs
    src/response/into_response_parts.rs
    src/response/mod.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/futures-core-0.3.34
    contrib/crates/http-1.5.0
    contrib/crates/http-body-1.1.0
    contrib/crates/http-body-util-0.1.5
    contrib/crates/mime-0.3.17
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/sync_wrapper-1.0.2
    contrib/crates/tower-layer-0.3.3
    contrib/crates/tower-service-0.3.3
)

LICENSE("MIT")
VERSION(0.5.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/axum-core/axum-core-0.5.6.crate)

CRATE_FEATURES(
    tracing
)

PEERDIR(
    contrib/crates/tracing-0.1.44
)

CRATE_SOURCES(
    src/body.rs
    src/error.rs
    src/ext_traits/mod.rs
    src/ext_traits/request.rs
    src/ext_traits/request_parts.rs
    src/extract/default_body_limit.rs
    src/extract/from_ref.rs
    src/extract/mod.rs
    src/extract/option.rs
    src/extract/rejection.rs
    src/extract/request_parts.rs
    src/extract/tuple.rs
    src/lib.rs
    src/macros.rs
    src/response/append_headers.rs
    src/response/into_response.rs
    src/response/into_response_parts.rs
    src/response/mod.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tracing-attributes-0.1.31
)

END()
