# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE_PROC_MACRO(mutants)

CRATE_VERSION(0.0.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.0.3)
LICENSE("MIT")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/lib.rs
)

END()
