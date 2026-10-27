# Native Rust build with dependencies shared through contrib/crates.

CRATE(synstructure)

CRATE_VERSION(0.14.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/macros.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    proc-macro
)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.9
)

LICENSE("MIT")
VERSION(0.14.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/synstructure/synstructure-0.14.0.crate)

END()
