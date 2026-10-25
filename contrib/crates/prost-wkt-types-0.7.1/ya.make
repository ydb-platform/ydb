# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(prost_wkt_types)

CRATE_VERSION(0.7.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_BUILD_TOOL(PROTOC contrib/tools/protoc)
CRATE_ENV(PROTOC_INCLUDE=${ARCADIA_ROOT}/contrib/libs/protobuf/src)

CRATE_SOURCES(
    build.rs
    src/pbany.rs
    src/pbempty.rs
    src/pbmask.rs
    src/pbstruct.rs
    src/pbtime/datetime.rs
    src/pbtime/duration.rs
    src/pbtime/mod.rs
    src/pbtime/timestamp.rs
)

CRATE_INPUTS(
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/any.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/descriptor.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/empty.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/field_mask.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/struct.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/duration.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/timestamp.proto
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    README.md
    proto/pbany.proto
    proto/pbempty.proto
    proto/pbmask.proto
    proto/pbstruct.proto
    proto/pbtime.proto
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/chrono-0.4.45
    contrib/crates/prost-0.14.4
    contrib/crates/prost-wkt-0.7.1
    contrib/crates/serde-1.0.229
    contrib/crates/serde_json-1.0.151
)

CRATE_BUILD_SCRIPT(contrib/crates/prost-wkt-types-0.7.1/build_script)

LICENSE("Apache-2.0")
VERSION(0.7.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-wkt-types/prost-wkt-types-0.7.1.crate)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
    contrib/crates/typetag-impl-0.2.23
)

END()

RECURSE(build_script)
