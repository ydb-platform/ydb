# Native Rust build with dependencies shared through contrib/crates.

CRATE(pkg_config)

CRATE_VERSION(0.3.34)
CRATE_EDITION(2021)
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
VERSION(0.3.34)
ORIGINAL_SOURCE(https://static.crates.io/crates/pkg-config/pkg-config-0.3.34.crate)

END()
