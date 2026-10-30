# Native Rust build with dependencies shared through contrib/crates.

CRATE(inventory)

CRATE_VERSION(0.3.25)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.25)
ORIGINAL_SOURCE(https://static.crates.io/crates/inventory/inventory-0.3.25.crate)

END()
