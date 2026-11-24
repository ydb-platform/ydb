# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(prost_derive)

CRATE_VERSION(0.14.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/field/group.rs
    src/field/map.rs
    src/field/message.rs
    src/field/mod.rs
    src/field/oneof.rs
    src/field/scalar.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/anyhow-1.0.104
    contrib/crates/itertools-0.14.0
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("Apache-2.0")
VERSION(0.14.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-derive/prost-derive-0.14.4.crate)

CRATE_SOURCES(
    src/field/group.rs
    src/field/map.rs
    src/field/message.rs
    src/field/mod.rs
    src/field/oneof.rs
    src/field/scalar.rs
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
