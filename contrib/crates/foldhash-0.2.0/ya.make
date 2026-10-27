# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(foldhash)

CRATE_VERSION(0.2.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/convenience.rs
    src/fast.rs
    src/quality.rs
    src/seed.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Zlib")
VERSION(0.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/foldhash/foldhash-0.2.0.crate)

CRATE_SOURCES(
    src/convenience.rs
    src/fast.rs
    src/lib.rs
    src/quality.rs
    src/seed.rs
)

END()
