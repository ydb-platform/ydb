# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(malloc_buf)

CRATE_VERSION(0.0.6)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.0.6)
LICENSE("MIT")


CRATE_INPUTS(.cargo-checksum.json)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/libc-0.2.191
)

END()
