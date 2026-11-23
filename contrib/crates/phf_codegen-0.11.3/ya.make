# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(phf_codegen)

CRATE_VERSION(0.11.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.11.3)
LICENSE("MIT")

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/phf_generator-0.11.3
    contrib/crates/phf_shared-0.13.1
)

CRATE_SOURCES(
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
