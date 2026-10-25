# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(thread_local)

CRATE_VERSION(1.1.10)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/cached.rs
    src/thread_id.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.10)
ORIGINAL_SOURCE(https://static.crates.io/crates/thread_local/thread_local-1.1.10.crate)

CRATE_SOURCES(
    src/cached.rs
    src/lib.rs
    src/thread_id.rs
)

END()
