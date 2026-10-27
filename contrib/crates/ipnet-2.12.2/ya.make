# Native Rust build with dependencies shared through contrib/crates.

CRATE(ipnet)

CRATE_VERSION(2.12.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ipext.rs
    src/ipnet.rs
    src/ipnet_schemars_08.rs
    src/ipnet_schemars_1.rs
    src/ipnet_serde.rs
    src/mask.rs
    src/parser.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(2.12.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/ipnet/ipnet-2.12.2.crate)

END()
