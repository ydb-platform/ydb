# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(utf8)

CRATE_VERSION(0.7.6)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.7.6)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/lossy.rs
    src/read.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
    src/lossy.rs
    src/read.rs
)

END()
