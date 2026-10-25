# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(fastrand)

CRATE_VERSION(2.5.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/global_rng.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

LICENSE("Apache-2.0 OR MIT")
VERSION(2.5.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/fastrand/fastrand-2.5.0.crate)

CRATE_SOURCES(
    src/global_rng.rs
    src/lib.rs
)

END()
