# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(version_check)

CRATE_VERSION(0.9.5)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/channel.rs
    src/date.rs
    src/version.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .travis.yml
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.9.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/version_check/version_check-0.9.5.crate)

CRATE_SOURCES(
    src/channel.rs
    src/date.rs
    src/lib.rs
    src/version.rs
)

END()
