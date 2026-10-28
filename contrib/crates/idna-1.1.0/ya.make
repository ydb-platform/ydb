# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(idna)

CRATE_VERSION(1.1.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(1.1.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/deprecated.rs
    src/punycode.rs
    src/uts46.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    compiled_data
    std
)
PEERDIR(
    contrib/crates/idna_adapter-1.2.2
    contrib/crates/smallvec-1.16.4
    contrib/crates/utf8_iter-1.0.4
)

CRATE_FEATURES(
    default
)

CRATE_SOURCES(
    src/deprecated.rs
    src/lib.rs
    src/punycode.rs
    src/uts46.rs
)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/serde_derive-1.0.229
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
