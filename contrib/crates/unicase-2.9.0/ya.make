# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(unicase)

CRATE_VERSION(2.9.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ascii.rs
    src/unicode/map.rs
    src/unicode/mod.rs
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
VERSION(2.9.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/unicase/unicase-2.9.0.crate)

END()
