# Native Rust build with dependencies shared through contrib/crates.

CRATE(cfg_if)

CRATE_VERSION(1.0.5)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/cfg-if/cfg-if-1.0.5.crate)

END()
