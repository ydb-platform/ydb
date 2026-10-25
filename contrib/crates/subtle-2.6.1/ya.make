# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(subtle)

CRATE_VERSION(2.6.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    CONTRIBUTING.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("BSD-3-Clause")
VERSION(2.6.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/subtle/subtle-2.6.1.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
