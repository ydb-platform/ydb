# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(prost_types)

CRATE_VERSION(0.14.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/any.rs
    src/compiler.rs
    src/conversions.rs
    src/datetime.rs
    src/duration.rs
    src/protobuf.rs
    src/timestamp.rs
    src/type_url.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/prost-0.14.4
)

LICENSE("Apache-2.0")
VERSION(0.14.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-types/prost-types-0.14.4.crate)

CRATE_SOURCES(
    src/any.rs
    src/compiler.rs
    src/conversions.rs
    src/datetime.rs
    src/duration.rs
    src/lib.rs
    src/protobuf.rs
    src/timestamp.rs
    src/type_url.rs
)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
