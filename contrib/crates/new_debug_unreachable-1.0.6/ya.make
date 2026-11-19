# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(debug_unreachable)

CRATE_VERSION(1.0.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.0.6)
LICENSE("MIT")

CRATE_SOURCES(examples/simple.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .travis.yml
    Cargo.lock
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
)

END()
