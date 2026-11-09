# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pin_project_lite)

CRATE_VERSION(0.2.17)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

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

LICENSE("Apache-2.0 OR MIT")
VERSION(0.2.17)
ORIGINAL_SOURCE(https://static.crates.io/crates/pin-project-lite/pin-project-lite-0.2.17.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
