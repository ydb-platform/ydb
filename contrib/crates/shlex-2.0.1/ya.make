# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(shlex)

CRATE_VERSION(2.0.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bytes.rs
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
    src/quoting_warning.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(2.0.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/shlex/shlex-2.0.1.crate)

CRATE_INPUTS(
    src/quoting_warning.md
)

CRATE_SOURCES(
    src/bytes.rs
    src/lib.rs
)

END()
