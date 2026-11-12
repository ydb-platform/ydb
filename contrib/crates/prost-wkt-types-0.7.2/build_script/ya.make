# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.7.2)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/lib.rs
    ../src/pbany.rs
    ../src/pbempty.rs
    ../src/pbmask.rs
    ../src/pbstruct.rs
    ../src/pbtime/datetime.rs
    ../src/pbtime/duration.rs
    ../src/pbtime/mod.rs
    ../src/pbtime/timestamp.rs
)

CRATE_INPUTS(
    ../Cargo.lock
    ../README.md
    ../proto/google/protobuf/README.md
    ../proto/google/protobuf/VERSION
    ../proto/google/protobuf/any.proto
    ../proto/google/protobuf/duration.proto
    ../proto/google/protobuf/empty.proto
    ../proto/google/protobuf/field_mask.proto
    ../proto/google/protobuf/struct.proto
    ../proto/google/protobuf/timestamp.proto
    ../proto/pbany.proto
    ../proto/pbempty.proto
    ../proto/pbmask.proto
    ../proto/pbstruct.proto
    ../proto/pbtime.proto
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/prost-0.14.4
    contrib/crates/prost-build-0.14.4
    contrib/crates/prost-types-0.14.4
    contrib/crates/prost-wkt-build-0.7.2
    contrib/crates/regex-1.13.1
)

LICENSE("Apache-2.0 AND BSD-3-Clause")
VERSION(0.7.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-wkt-types/prost-wkt-types-0.7.2.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
