# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pulldown_cmark_to_cmark)

CRATE_VERSION(22.0.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/source_range.rs
    src/text_modifications.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/pulldown-cmark-0.13.4
)

LICENSE("Apache-2.0")
VERSION(22.0.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pulldown-cmark-to-cmark/pulldown-cmark-to-cmark-22.0.0.crate)

END()
