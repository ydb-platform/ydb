# Native Rust build with dependencies shared through contrib/crates.

CRATE(zerovec)

CRATE_VERSION(0.11.9)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/cow.rs
    src/hashmap/algorithms.rs
    src/hashmap/mod.rs
    src/hashmap/serde.rs
    src/map/borrowed.rs
    src/map/databake.rs
    src/map/kv.rs
    src/map/map.rs
    src/map/mod.rs
    src/map/serde.rs
    src/map/serde_helpers.rs
    src/map/vecs.rs
    src/map2d/borrowed.rs
    src/map2d/cursor.rs
    src/map2d/databake.rs
    src/map2d/map.rs
    src/map2d/mod.rs
    src/map2d/serde.rs
    src/samples.rs
    src/schemars.rs
    src/ule/chars.rs
    src/ule/custom.rs
    src/ule/encode.rs
    src/ule/macros.rs
    src/ule/mod.rs
    src/ule/multi.rs
    src/ule/niche.rs
    src/ule/option.rs
    src/ule/plain.rs
    src/ule/slices.rs
    src/ule/test_utils.rs
    src/ule/tuple.rs
    src/ule/tuplevar.rs
    src/ule/vartuple.rs
    src/varzerovec/components.rs
    src/varzerovec/databake.rs
    src/varzerovec/error.rs
    src/varzerovec/lengthless.rs
    src/varzerovec/mod.rs
    src/varzerovec/owned.rs
    src/varzerovec/serde.rs
    src/varzerovec/slice.rs
    src/varzerovec/vec.rs
    src/yoke_impls.rs
    src/zerofrom_impls.rs
    src/zerovec/databake.rs
    src/zerovec/mod.rs
    src/zerovec/serde.rs
    src/zerovec/slice.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    src/snapshots/zerovec__schemars__tests__schema_varzerovec_str.snap
    src/snapshots/zerovec__schemars__tests__schema_varzerovec_zeroslice.snap
    src/snapshots/zerovec__schemars__tests__schema_zeromap_u32_str.snap
    src/snapshots/zerovec__schemars__tests__schema_zerovec_char.snap
    src/snapshots/zerovec__schemars__tests__schema_zerovec_u32.snap
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    derive
    yoke
)

PEERDIR(
    contrib/crates/yoke-0.8.4
    contrib/crates/zerofrom-0.1.8
)

LICENSE("Unicode-3.0")
VERSION(0.11.9)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerovec/zerovec-0.11.9.crate)

CRATE_FEATURES(
    alloc
)

CRATE_PROC_MACROS(
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
