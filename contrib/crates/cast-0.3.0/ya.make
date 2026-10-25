# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(cast)

CRATE_VERSION(0.3.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(src/test.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
    src/test.rs
)

END()
