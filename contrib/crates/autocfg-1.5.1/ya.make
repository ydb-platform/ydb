# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(autocfg)

CRATE_VERSION(1.5.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/rustc.rs
    src/tests.rs
    src/version.rs
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

LICENSE("Apache-2.0 OR MIT")
VERSION(1.5.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/autocfg/autocfg-1.5.1.crate)

CRATE_SOURCES(
    src/error.rs
    src/lib.rs
    src/rustc.rs
    src/tests.rs
    src/version.rs
)


END()
