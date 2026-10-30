# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(fxhash)

CRATE_VERSION(0.2.1)
CRATE_EDITION(2015)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.2.1)
LICENSE("Apache-2.0 OR MIT")

CRATE_SOURCES(bench.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/byteorder-1.5.0
)

END()
