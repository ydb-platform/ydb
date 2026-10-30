# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(cssparser_color)

CRATE_VERSION(0.3.0)
CRATE_EDITION(2021)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.0)
LICENSE("MPL-2.0")

CRATE_SOURCES(tests.rs)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cssparser-0.35.0
)

CRATE_PROC_MACROS(
    contrib/crates/cssparser-macros-0.6.1
    contrib/crates/phf_macros-0.11.3
    contrib/crates/serde_derive-1.0.229
    contrib/crates/zerocopy-derive-0.8.63
)

END()
