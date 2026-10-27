# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(fragile)

CRATE_VERSION(2.1.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/errors.rs
    src/fragile.rs
    src/futures.rs
    src/registry.rs
    src/semisticky.rs
    src/sticky.rs
    src/thread_id.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    Cargo.lock.msrv
    LICENSE
    Makefile
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    future
    futures-core
    stream
)

PEERDIR(
    contrib/crates/futures-core-0.3.34
)

LICENSE("Apache-2.0")
VERSION(2.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/fragile/fragile-2.1.0.crate)

CRATE_SOURCES(
    src/errors.rs
    src/fragile.rs
    src/futures.rs
    src/lib.rs
    src/registry.rs
    src/semisticky.rs
    src/sticky.rs
    src/thread_id.rs
)

END()
