# Native Rust build with dependencies shared through contrib/crates.

CRATE(serde_path_to_error)

CRATE_VERSION(0.1.20)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/de.rs
    src/path.rs
    src/ser.rs
    src/wrap.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/itoa-1.0.18
    contrib/crates/serde_core-1.0.229
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.20)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde_path_to_error/serde_path_to_error-0.1.20.crate)

END()
