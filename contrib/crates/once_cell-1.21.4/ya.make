# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(once_cell)

CRATE_VERSION(1.21.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/imp_cs.rs
    src/imp_pl.rs
    src/imp_std.rs
    src/race.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    bors.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    race
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.21.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/once_cell/once_cell-1.21.4.crate)

CRATE_SOURCES(
    src/imp_cs.rs
    src/imp_pl.rs
    src/imp_std.rs
)

CRATE_SOURCES(
    src/lib.rs
    src/race.rs
)

END()
