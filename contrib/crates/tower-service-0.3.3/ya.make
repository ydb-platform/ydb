# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tower_service)

CRATE_VERSION(0.3.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.3.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/tower-service/tower-service-0.3.3.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
