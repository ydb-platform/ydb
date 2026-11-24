# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(try_lock)

CRATE_VERSION(0.2.5)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.2.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/try-lock/try-lock-0.2.5.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
