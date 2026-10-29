# Native Rust build with dependencies shared through contrib/crates.

CRATE(lazy_static)

CRATE_VERSION(1.5.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/core_lazy.rs
    src/inline_lazy.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.5.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/lazy_static/lazy_static-1.5.1.crate)

END()
