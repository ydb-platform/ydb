# Native Rust build with dependencies shared through contrib/crates.

CRATE(tinyvec)

CRATE_VERSION(1.11.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/array.rs
    src/array/const_generic_impl.rs
    src/array/generic_array_impl.rs
    src/arrayvec.rs
    src/arrayvec_drain.rs
    src/slicevec.rs
    src/tinyvec.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE.md
    LICENSE-MIT.md
    LICENSE-ZLIB.md
    README.md
    debug_metadata/README.md
    debug_metadata/tinyvec.natvis
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    tinyvec_macros
)

PEERDIR(
    contrib/crates/tinyvec_macros-0.1.1
)

LICENSE("Zlib OR Apache-2.0 OR MIT")
VERSION(1.11.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/tinyvec/tinyvec-1.11.0.crate)

CRATE_FEATURES(
    rustc_1_55
)

END()
