# Native Rust build with dependencies shared through contrib/crates.

CRATE(data_encoding)

CRATE_VERSION(2.11.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

LICENSE("MIT")
VERSION(2.11.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/data-encoding/data-encoding-2.11.1.crate)

END()
