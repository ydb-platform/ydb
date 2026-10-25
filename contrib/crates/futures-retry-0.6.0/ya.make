# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(futures_retry)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error_handler.rs
    src/future.rs
    src/stream.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .gitlab-ci.yml
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/futures-0.3.34
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/tokio-1.54.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/futures-retry/futures-retry-0.6.0.crate)

CRATE_SOURCES(
    src/error_handler.rs
    src/future.rs
    src/lib.rs
    src/stream.rs
)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
)

END()
