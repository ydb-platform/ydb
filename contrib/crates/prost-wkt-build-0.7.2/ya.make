# Native Rust build with dependencies shared through contrib/crates.

CRATE(prost_wkt_build)

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
    contrib/crates/heck-0.5.0
    contrib/crates/prost-0.14.4
    contrib/crates/prost-build-0.14.4
    contrib/crates/prost-types-0.14.4
    contrib/crates/quote-1.0.47
)

LICENSE("Apache-2.0")
VERSION(0.7.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-wkt-build/prost-wkt-build-0.7.2.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
