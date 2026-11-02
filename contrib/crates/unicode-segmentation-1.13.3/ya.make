# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(unicode_segmentation)

CRATE_VERSION(1.13.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/grapheme.rs
    src/sentence.rs
    src/tables.rs
    src/word.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    COPYRIGHT
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.13.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/unicode-segmentation/unicode-segmentation-1.13.3.crate)

CRATE_SOURCES(
    src/grapheme.rs
    src/lib.rs
    src/sentence.rs
    src/tables.rs
    src/word.rs
)

END()
