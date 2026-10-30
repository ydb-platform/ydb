# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(cssparser)

CRATE_VERSION(0.35.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.35.0)
LICENSE("MPL-2.0")

CRATE_SOURCES(
    src/color.rs
    src/cow_rc_str.rs
    src/from_bytes.rs
    src/macros.rs
    src/nth.rs
    src/parser.rs
    src/rules_and_declarations.rs
    src/serializer.rs
    src/size_of_tests.rs
    src/tests.rs
    src/tokenizer.rs
    src/unicode_range.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
    README.md
    docs/.nojekyll
    docs/404.html
    docs/index.html
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/dtoa-short-0.3.5
    contrib/crates/itoa-1.0.18
    contrib/crates/phf-0.11.3
    contrib/crates/smallvec-1.16.4
)

CRATE_PROC_MACROS(
    contrib/crates/cssparser-macros-0.6.1
    contrib/crates/phf_macros-0.11.3
    contrib/crates/serde_derive-1.0.229
    contrib/crates/zerocopy-derive-0.8.63
)

END()
