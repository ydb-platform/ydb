# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(form_urlencoded)

CRATE_VERSION(1.2.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.2.2)
LICENSE("MIT OR Apache-2.0")


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
PEERDIR(
    contrib/crates/percent-encoding-2.3.2
)

CRATE_FEATURES(
    default
)

CRATE_SOURCES(
    src/lib.rs
)

END()
