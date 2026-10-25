# Native Rust build with dependencies shared through contrib/crates.

CRATE(num_integer)

CRATE_VERSION(0.1.47)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/average.rs
    src/roots.rs
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
    i128
    std
)

PEERDIR(
    contrib/crates/num-traits-0.2.19
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.47)
ORIGINAL_SOURCE(https://static.crates.io/crates/num-integer/num-integer-0.1.47.crate)

CRATE_FEATURES(
    default
)

END()
