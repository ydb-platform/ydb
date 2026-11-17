# Native Rust build with dependencies shared through contrib/crates.

CRATE(prost_wkt)

CRATE_VERSION(0.7.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/chrono-0.4.45
    contrib/crates/inventory-0.3.25
    contrib/crates/prost-0.14.4
    contrib/crates/serde-1.0.229
    contrib/crates/serde_json-1.0.154
    contrib/crates/typetag-0.2.23
)

LICENSE("Apache-2.0")
VERSION(0.7.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-wkt/prost-wkt-0.7.2.crate)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
    contrib/crates/typetag-impl-0.2.23
)

END()
