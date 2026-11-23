# Native Rust build with dependencies shared through contrib/crates.

CRATE(pulldown_cmark_to_cmark)

CRATE_VERSION(22.0.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/source_range.rs
    src/text_modifications.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/pulldown-cmark-0.13.5
)

LICENSE("Apache-2.0")
VERSION(22.0.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/pulldown-cmark-to-cmark/pulldown-cmark-to-cmark-22.0.3.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
