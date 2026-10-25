# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(string_cache)

CRATE_VERSION(0.9.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.9.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/simple.rs
    src/atom.rs
    src/dynamic_set.rs
    src/static_sets.rs
    src/trivial_impls.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    serde
    serde_support
)
PEERDIR(
    contrib/crates/new_debug_unreachable-1.0.6
    contrib/crates/parking_lot-0.12.5
    contrib/crates/phf_shared-0.13.1
    contrib/crates/precomputed-hash-0.1.1
    contrib/crates/serde-1.0.229
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
