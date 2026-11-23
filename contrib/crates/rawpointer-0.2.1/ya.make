# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(rawpointer)

CRATE_VERSION(0.2.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.2.1)
LICENSE("MIT OR Apache-2.0")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .travis.yml
    LICENSE-APACHE
    LICENSE-MIT
    README.rst
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
)

END()
