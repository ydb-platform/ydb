# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(dtoa)

CRATE_VERSION(1.0.11)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.0.11)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/diyfp.rs
    src/dtoa.rs
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




END()
