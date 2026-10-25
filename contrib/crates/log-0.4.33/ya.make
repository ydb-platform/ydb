# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(log)

CRATE_VERSION(0.4.33)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/__private_api.rs
    src/kv/error.rs
    src/kv/key.rs
    src/kv/mod.rs
    src/kv/source.rs
    src/kv/value.rs
    src/macros.rs
    src/serde.rs
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
    triagebot.toml
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.33)
ORIGINAL_SOURCE(https://static.crates.io/crates/log/log-0.4.33.crate)

END()
