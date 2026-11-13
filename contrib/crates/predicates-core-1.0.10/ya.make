# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(predicates_core)

CRATE_VERSION(1.0.10)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/core.rs
    src/reflection.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.10)
ORIGINAL_SOURCE(https://static.crates.io/crates/predicates-core/predicates-core-1.0.10.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/core.rs
    src/lib.rs
    src/reflection.rs
)

END()
