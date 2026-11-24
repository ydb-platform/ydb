# Native Rust build with dependencies shared through contrib/crates.

CRATE(sha1_smol)

CRATE_VERSION(1.0.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/simd.rs
)

CRATE_INPUTS(
    AUTHORS
    LICENSE
    Makefile
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("BSD-3-Clause")
VERSION(1.0.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/sha1_smol/sha1_smol-1.0.1.crate)

END()
