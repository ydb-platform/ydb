# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(xml5ever)

CRATE_VERSION(0.35.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.35.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/simple_xml_tokenizer.rs
    examples/xml_tokenizer.rs
    src/driver.rs
    src/macros.rs
    src/serialize/mod.rs
    src/tokenizer/char_ref/mod.rs
    src/tokenizer/interface.rs
    src/tokenizer/mod.rs
    src/tokenizer/qname.rs
    src/tokenizer/states.rs
    src/tree_builder/mod.rs
    src/tree_builder/types.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    data/bench/strong.xml
    examples/README.md
    examples/example.xml
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/log-0.4.34
    contrib/crates/markup5ever-0.35.0
)

CRATE_PROC_MACROS(
    contrib/crates/phf_macros-0.11.3
    contrib/crates/serde_derive-1.0.229
    contrib/crates/zerocopy-derive-0.8.63
)

END()
