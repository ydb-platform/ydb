# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(bzip2)

CRATE_VERSION(0.6.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bufread.rs
    src/mem.rs
    src/read.rs
    src/write.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    SECURITY.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

PEERDIR(
    contrib/crates/libbz2-rs-sys-0.2.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/bzip2/bzip2-0.6.1.crate)

CRATE_INPUTS(
    tests/bug_61.bz2
    tests/bug_61.raw
)

CRATE_SOURCES(
    src/bufread.rs
    src/lib.rs
    src/mem.rs
    src/read.rs
    src/write.rs
)

END()
