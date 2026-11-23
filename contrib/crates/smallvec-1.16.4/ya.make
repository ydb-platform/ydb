# Native Rust build with dependencies shared through contrib/crates.

CRATE(smallvec)

CRATE_VERSION(1.16.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/arbitrary.rs
    src/specialization.rs
    src/tests.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    const_generics
    const_new
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.16.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/smallvec/smallvec-1.16.4.crate)

CRATE_FEATURES(
    serde
    union
    write
)

PEERDIR(
    contrib/crates/serde-1.0.229
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
