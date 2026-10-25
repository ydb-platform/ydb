# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(utf8_iter)

CRATE_VERSION(1.0.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.0.4)
LICENSE("Apache-2.0 OR MIT")

CRATE_SOURCES(
    src/indices.rs
    src/report.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    COPYRIGHT
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/indices.rs
    src/lib.rs
    src/report.rs
)

END()
