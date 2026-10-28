# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(percent_encoding)

CRATE_VERSION(2.3.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(2.3.2)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(src/ascii_set.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    alloc
    std
)

CRATE_FEATURES(
    default
)

CRATE_SOURCES(
    src/ascii_set.rs
    src/lib.rs
)

END()
