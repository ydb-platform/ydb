# Native Rust build with dependencies shared through contrib/crates.

CRATE(atomic)

CRATE_VERSION(0.6.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/fallback.rs
    src/ops.rs
    src/serde_impl.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bytemuck-1.25.2
)

LICENSE("Apache-2.0 OR MIT")
VERSION(0.6.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/atomic/atomic-0.6.1.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
