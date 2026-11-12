# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(block)

CRATE_VERSION(0.1.6)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.6)
LICENSE("MIT")

CRATE_SOURCES(src/test_utils.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




END()
