# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(precomputed_hash)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.1)
LICENSE("MIT")


CRATE_INPUTS(
    .cargo-checksum.json
    LICENSE
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
)

END()
