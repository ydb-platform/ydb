# Native Rust build with dependencies shared through contrib/crates.

CRATE(foldhash)

CRATE_VERSION(0.1.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/convenience.rs
    src/fast.rs
    src/quality.rs
    src/seed.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("Zlib")
VERSION(0.1.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/foldhash/foldhash-0.1.5.crate)

END()
