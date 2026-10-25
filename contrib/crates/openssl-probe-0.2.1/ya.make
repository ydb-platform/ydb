# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(openssl_probe)

CRATE_VERSION(0.2.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    deny.toml
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl-probe/openssl-probe-0.2.1.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
