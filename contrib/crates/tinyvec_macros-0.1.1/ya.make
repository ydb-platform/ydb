# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(tinyvec_macros)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.1)
LICENSE("MIT OR Apache-2.0 OR Zlib")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE.md
    LICENSE-MIT.md
    LICENSE-ZLIB.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
)

END()
