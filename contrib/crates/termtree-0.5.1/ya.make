# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(termtree)

CRATE_VERSION(0.5.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/tests.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.5.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/termtree/termtree-0.5.1.crate)

CRATE_SOURCES(
    src/lib.rs
    src/tests.rs
)

END()
