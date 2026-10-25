# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(httpdate)

CRATE_VERSION(1.0.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/date.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/httpdate/httpdate-1.0.3.crate)

CRATE_SOURCES(
    src/date.rs
    src/lib.rs
)

END()
