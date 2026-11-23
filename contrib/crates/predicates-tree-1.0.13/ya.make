# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(predicates_tree)

CRATE_VERSION(1.0.13)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/predicates-core-1.0.10
    contrib/crates/termtree-0.5.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.13)
ORIGINAL_SOURCE(https://static.crates.io/crates/predicates-tree/predicates-tree-1.0.13.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/lib.rs
)

END()
