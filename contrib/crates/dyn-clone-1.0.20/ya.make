# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(dyn_clone)

CRATE_VERSION(1.0.20)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/macros.rs
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
VERSION(1.0.20)
ORIGINAL_SOURCE(https://static.crates.io/crates/dyn-clone/dyn-clone-1.0.20.crate)

CRATE_SOURCES(
    src/lib.rs
    src/macros.rs
)

END()
