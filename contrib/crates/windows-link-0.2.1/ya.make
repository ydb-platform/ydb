# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(windows_link)

CRATE_VERSION(0.2.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.2.1)
LICENSE("MIT OR Apache-2.0")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/lib.rs
)

END()
