# Native Rust build with dependencies shared through contrib/crates.

CRATE(prost_wkt_types)

CRATE_VERSION(0.7.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

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
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/duration.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/empty.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/field_mask.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/struct.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/timestamp.proto
    ${ARCADIA_ROOT}/contrib/libs/protobuf/src/google/protobuf/wrappers.proto
    Cargo.lock
    README.md
    proto/google/protobuf/README.md
    proto/google/protobuf/VERSION
    proto/google/protobuf/any.proto
    proto/google/protobuf/duration.proto
    proto/google/protobuf/empty.proto
    proto/google/protobuf/field_mask.proto
    proto/google/protobuf/struct.proto
    proto/google/protobuf/timestamp.proto
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
    contrib/crates/prost-wkt-0.7.2
    contrib/crates/serde-1.0.229
    contrib/crates/serde_json-1.0.154
)

CRATE_BUILD_SCRIPT(contrib/crates/prost-wkt-types-0.7.2/build_script)

LICENSE("Apache-2.0 AND BSD-3-Clause")
VERSION(0.7.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-wkt-types/prost-wkt-types-0.7.2.crate)

CRATE_BUILD_TOOL(PROTOC contrib/tools/protoc)
CRATE_ENV(PROTOC_INCLUDE=${ARCADIA_ROOT}/contrib/libs/protobuf/src)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
    contrib/crates/typetag-impl-0.2.23
)

END()

RECURSE(build_script)
