# Native Rust build with dependencies shared through contrib/crates.

CRATE(equivalent)

CRATE_VERSION(1.0.3)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.0.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/equivalent/equivalent-1.0.3.crate)

END()
