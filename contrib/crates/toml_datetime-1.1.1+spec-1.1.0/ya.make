# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(toml_datetime)

CRATE_VERSION(1.1.1+spec-1.1.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/datetime.rs
    src/de.rs
    src/ser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    serde
    std
)

PEERDIR(
    contrib/crates/serde_core-1.0.229
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.1+spec-1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/toml_datetime/toml_datetime-1.1.1+spec-1.1.0.crate)

END()
