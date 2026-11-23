# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(objc_id)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.1)
LICENSE("MIT")

CRATE_SOURCES(src/id.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/objc-0.2.7
)

END()
