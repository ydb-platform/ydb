# Native Rust build; versions and features follow temporalio 1.32.0 Cargo.lock.

CRATE(temporalio_common_wasm)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/activity_definition.rs
    src/data_converters.rs
    src/data_converters/failure_converter.rs
    src/error.rs
    src/memo.rs
    src/priority.rs
    src/retry_policy.rs
    src/search_attributes.rs
    src/worker.rs
    src/workflow_definition.rs
    src/workflow_execution.rs
)

CRATE_INPUTS(
    LICENSE.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    serde_serialize
)

PEERDIR(
    contrib/crates/anyhow-1.0.104
    contrib/crates/bon-3.9.3
    contrib/crates/chrono-0.4.45
    contrib/crates/crc32fast-1.5.3
    contrib/crates/derive_more-2.1.1
    contrib/crates/erased-serde-0.4.10
    contrib/crates/futures-0.3.34
    contrib/crates/parking_lot-0.12.5
    contrib/crates/prost-0.14.4
    contrib/crates/prost-wkt-types-0.7.2
    contrib/crates/serde-1.0.229
    contrib/crates/serde_json-1.0.154
    contrib/crates/thiserror-2.0.21
    contrib/crates/tracing-0.1.44
    contrib/crates/tracing-core-0.1.36
    contrib/crates/tracing-subscriber-0.3.23
    contrib/crates/url-2.5.8
    contrib/python/temporalio/temporalio/bridge/sdk-core/crates/protos
)

CRATE_DEPENDENCY_ALIAS(prost_types contrib/crates/prost-wkt-types-0.7.2)
LICENSE(MIT)
VERSION(0.6.0)

CRATE_PROC_MACROS(
    contrib/crates/async-trait-0.1.92
    contrib/crates/bon-macros-3.9.3
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/darling_macro-0.23.0
    contrib/crates/derive_more-impl-2.1.1
    contrib/crates/displaydoc-0.2.7
    contrib/crates/futures-macro-0.3.34
    contrib/crates/pin-project-internal-1.1.13
    contrib/crates/prost-derive-0.14.4
    contrib/crates/rustversion-1.0.23
    contrib/crates/serde_derive-1.0.229
    contrib/crates/thiserror-impl-2.0.21
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
    contrib/crates/typetag-impl-0.2.23
    contrib/crates/valuable-derive-0.1.1
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerocopy-derive-0.8.63
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
