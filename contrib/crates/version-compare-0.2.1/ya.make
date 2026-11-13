# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(version_compare)

CRATE_VERSION(0.2.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.2.1)
LICENSE("MIT")

CRATE_SOURCES(
    examples/example.rs
    examples/minimal.rs
    src/cmp.rs
    src/compare.rs
    src/manifest.rs
    src/part.rs
    src/test.rs
    src/version.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




END()
