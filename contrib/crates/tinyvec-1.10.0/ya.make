# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(tinyvec)

CRATE_VERSION(1.10.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.10.0)
LICENSE("Zlib OR Apache-2.0 OR MIT")

CRATE_SOURCES(
    src/array/const_generic_impl.rs
    src/array/generic_array_impl.rs
    src/array.rs
    src/arrayvec.rs
    src/arrayvec_drain.rs
    src/slicevec.rs
    src/tinyvec.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
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
    rustc_1_55
    tinyvec_macros
)
PEERDIR(contrib/crates/tinyvec_macros-0.1.1)

END()
