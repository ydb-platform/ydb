# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(data_url)

CRATE_VERSION(0.3.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.2)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/forgiving_base64.rs
    src/mime.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    src/make_base64_decode_table.py
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    alloc
    default
    std
)

END()
