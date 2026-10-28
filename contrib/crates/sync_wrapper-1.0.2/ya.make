# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(sync_wrapper)

CRATE_VERSION(1.0.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    futures
    futures-core
)

PEERDIR(
    contrib/crates/futures-core-0.3.34
)

LICENSE("Apache-2.0")
VERSION(1.0.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/sync_wrapper/sync_wrapper-1.0.2.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
