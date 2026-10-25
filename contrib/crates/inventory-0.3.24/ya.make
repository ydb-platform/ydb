# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(inventory)

CRATE_VERSION(0.3.24)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.24)
ORIGINAL_SOURCE(https://static.crates.io/crates/inventory/inventory-0.3.24.crate)

END()
