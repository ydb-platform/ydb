# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(smallvec)

CRATE_VERSION(1.15.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/arbitrary.rs
    src/specialization.rs
    src/tests.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    debug_metadata/README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    const_generics
    const_new
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.15.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/smallvec/smallvec-1.15.2.crate)

CRATE_FEATURES(
    union
)

END()
