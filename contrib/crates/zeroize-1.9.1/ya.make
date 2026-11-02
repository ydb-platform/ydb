# Native Rust build with dependencies shared through contrib/crates.

CRATE(zeroize)

CRATE_VERSION(1.9.1)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/aarch64.rs
    src/barrier.rs
    src/stack.rs
    src/x86.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.9.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/zeroize/zeroize-1.9.1.crate)

END()
