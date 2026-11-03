# Native Rust build with dependencies shared through contrib/crates.

CRATE(core_detect)

CRATE_VERSION(1.0.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/arch/x86.rs
    src/cache.rs
    src/macros.rs
    src/os/x86.rs
)

CRATE_INPUTS(
    Cross.toml
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/core_detect/core_detect-1.0.0.crate)

END()
