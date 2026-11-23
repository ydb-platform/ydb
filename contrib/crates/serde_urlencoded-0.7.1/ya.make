# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(serde_urlencoded)

CRATE_VERSION(0.7.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/de.rs
    src/ser/key.rs
    src/ser/mod.rs
    src/ser/pair.rs
    src/ser/part.rs
    src/ser/value.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/form_urlencoded-1.2.2
    contrib/crates/itoa-1.0.18
    contrib/crates/ryu-1.0.23
    contrib/crates/serde-1.0.229
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.7.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde_urlencoded/serde_urlencoded-0.7.1.crate)

CRATE_SOURCES(
    src/de.rs
    src/lib.rs
    src/ser/key.rs
    src/ser/mod.rs
    src/ser/pair.rs
    src/ser/part.rs
    src/ser/value.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
