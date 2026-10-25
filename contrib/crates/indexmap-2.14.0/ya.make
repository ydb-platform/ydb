# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(indexmap)

CRATE_VERSION(2.14.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/arbitrary.rs
    src/borsh.rs
    src/inner.rs
    src/inner/entry.rs
    src/inner/extract.rs
    src/macros.rs
    src/map.rs
    src/map/entry.rs
    src/map/iter.rs
    src/map/mutable.rs
    src/map/raw_entry_v1.rs
    src/map/serde_seq.rs
    src/map/slice.rs
    src/map/tests.rs
    src/rayon/map.rs
    src/rayon/mod.rs
    src/rayon/set.rs
    src/serde.rs
    src/set.rs
    src/set/iter.rs
    src/set/mutable.rs
    src/set/slice.rs
    src/set/tests.rs
    src/sval.rs
    src/util.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/equivalent-1.0.2
    contrib/crates/hashbrown-0.17.1
)

LICENSE("Apache-2.0 OR MIT")
VERSION(2.14.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/indexmap/indexmap-2.14.0.crate)

END()
