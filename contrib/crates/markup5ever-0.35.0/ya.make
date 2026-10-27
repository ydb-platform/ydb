# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(markup5ever)

CRATE_VERSION(0.35.0)
CRATE_EDITION(2021)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.35.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    interface/mod.rs
    interface/tree_builder.rs
    serialize.rs
    util/buffer_queue.rs
    util/smallcharset.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/log-0.4.34
    contrib/crates/tendril-0.4.3
    contrib/crates/web_atoms-0.1.3
)

CRATE_PROC_MACROS(
    contrib/crates/phf_macros-0.11.3
    contrib/crates/serde_derive-1.0.229
    contrib/crates/zerocopy-derive-0.8.63
)

END()
