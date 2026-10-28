# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(adler2)

CRATE_VERSION(2.0.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(2.0.1)
LICENSE("0BSD OR MIT OR Apache-2.0")

CRATE_SOURCES(src/algo.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-0BSD
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASE_PROCESS.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/algo.rs
    src/lib.rs
)

END()
