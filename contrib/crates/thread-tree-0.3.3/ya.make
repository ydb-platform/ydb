# Native Rust build with dependencies shared through contrib/crates.

CRATE(thread_tree)

CRATE_VERSION(0.3.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/job.rs
    src/unwind.rs
)

CRATE_INPUTS(
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/crossbeam-channel-0.5.17
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/thread-tree/thread-tree-0.3.3.crate)

END()
