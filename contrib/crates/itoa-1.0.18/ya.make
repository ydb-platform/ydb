# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(itoa)

CRATE_VERSION(1.0.18)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/u128_ext.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.18)
ORIGINAL_SOURCE(https://static.crates.io/crates/itoa/itoa-1.0.18.crate)

CRATE_SOURCES(
    src/lib.rs
    src/u128_ext.rs
)

END()
