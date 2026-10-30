# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(quick_error)

CRATE_VERSION(2.0.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(2.0.1)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(examples/context.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .travis.yml
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.rst
    bulk.yaml
    vagga.yaml
)

CRATE_FLAGS(--cap-lints=allow)




END()
