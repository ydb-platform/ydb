# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE_PROC_MACRO(indoc)

CRATE_VERSION(1.0.9)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/expr.rs
    src/unindent.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo/config
    .cargo_vcs_info.json
    .clippy.toml
    .gitignore
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.9)
ORIGINAL_SOURCE(https://static.crates.io/crates/indoc/indoc-1.0.9.crate)

CRATE_SOURCES(
    src/error.rs
    src/expr.rs
    src/lib.rs
    src/unindent.rs
)

END()
