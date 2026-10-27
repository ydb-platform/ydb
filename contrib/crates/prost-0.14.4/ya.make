# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(prost)

CRATE_VERSION(0.14.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/encoding.rs
    src/encoding/length_delimiter.rs
    src/encoding/varint.rs
    src/encoding/wire_type.rs
    src/error.rs
    src/message.rs
    src/name.rs
    src/types.rs
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
    derive
    std
)

PEERDIR(
    contrib/crates/bytes-1.12.1
)

LICENSE("Apache-2.0")
VERSION(0.14.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost/prost-0.14.4.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/encoding.rs
    src/encoding/length_delimiter.rs
    src/encoding/varint.rs
    src/encoding/wire_type.rs
    src/error.rs
    src/lib.rs
    src/message.rs
    src/name.rs
    src/types.rs
)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
