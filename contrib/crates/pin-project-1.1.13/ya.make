# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pin_project)

CRATE_VERSION(1.1.13)
CRATE_EDITION(2021)
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
VERSION(1.1.13)
ORIGINAL_SOURCE(https://static.crates.io/crates/pin-project/pin-project-1.1.13.crate)

CRATE_SOURCES(
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/pin-project-internal-1.1.13
)

END()
