# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(url)

CRATE_VERSION(2.5.8)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(2.5.8)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/host.rs
    src/origin.rs
    src/parser.rs
    src/path_segments.rs
    src/quirks.rs
    src/slicing.rs
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
    default
    std
)
PEERDIR(
    contrib/crates/form_urlencoded-1.2.2
    contrib/crates/idna-1.1.0
    contrib/crates/percent-encoding-2.3.2
)
CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.6
    contrib/crates/yoke-derive-0.8.2
    contrib/crates/zerofrom-derive-0.1.7
    contrib/crates/zerovec-derive-0.11.3
)

END()
