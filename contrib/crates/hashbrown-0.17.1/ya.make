# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(hashbrown)

CRATE_VERSION(0.17.1)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alloc.rs
    src/control/bitmask.rs
    src/control/group/generic.rs
    src/control/group/lsx.rs
    src/control/group/mod.rs
    src/control/group/neon.rs
    src/control/group/sse2.rs
    src/control/mod.rs
    src/control/tag.rs
    src/external_trait_impls/mod.rs
    src/external_trait_impls/rayon/helpers.rs
    src/external_trait_impls/rayon/map.rs
    src/external_trait_impls/rayon/mod.rs
    src/external_trait_impls/rayon/raw.rs
    src/external_trait_impls/rayon/set.rs
    src/external_trait_impls/rayon/table.rs
    src/external_trait_impls/serde.rs
    src/hasher.rs
    src/macros.rs
    src/map.rs
    src/raw.rs
    src/raw_entry.rs
    src/rustc_entry.rs
    src/scopeguard.rs
    src/set.rs
    src/table.rs
    src/util.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    Cross.toml
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    clippy.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    allocator-api2
    default
    default-hasher
    equivalent
    inline-more
    raw-entry
)

PEERDIR(
    contrib/crates/allocator-api2-0.2.21
    contrib/crates/equivalent-1.0.3
    contrib/crates/foldhash-0.2.0
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.17.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/hashbrown/hashbrown-0.17.1.crate)

CRATE_FEATURES(
    serde
)

PEERDIR(
    contrib/crates/serde_core-1.0.229
)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/alloc.rs
    src/control/bitmask.rs
    src/control/group/generic.rs
    src/control/group/lsx.rs
    src/control/group/mod.rs
    src/control/group/neon.rs
    src/control/group/sse2.rs
    src/control/mod.rs
    src/control/tag.rs
    src/external_trait_impls/mod.rs
    src/external_trait_impls/rayon/helpers.rs
    src/external_trait_impls/rayon/map.rs
    src/external_trait_impls/rayon/mod.rs
    src/external_trait_impls/rayon/raw.rs
    src/external_trait_impls/rayon/set.rs
    src/external_trait_impls/rayon/table.rs
    src/external_trait_impls/serde.rs
    src/hasher.rs
    src/lib.rs
    src/macros.rs
    src/map.rs
    src/raw.rs
    src/raw_entry.rs
    src/rustc_entry.rs
    src/scopeguard.rs
    src/set.rs
    src/table.rs
    src/util.rs
)

END()
