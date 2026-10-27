# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(atomic_waker)

CRATE_VERSION(1.1.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-THIRD-PARTY
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.1.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/atomic-waker/atomic-waker-1.1.2.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
