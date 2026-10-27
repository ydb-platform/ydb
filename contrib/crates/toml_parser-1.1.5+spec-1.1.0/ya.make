# Native Rust build with dependencies shared through contrib/crates.

CRATE(toml_parser)

CRATE_VERSION(1.1.5+spec-1.1.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/debug.rs
    src/decoder/mod.rs
    src/decoder/scalar.rs
    src/decoder/string.rs
    src/decoder/ws.rs
    src/error.rs
    src/lexer/mod.rs
    src/lexer/test.rs
    src/lexer/token.rs
    src/macros.rs
    src/parser/document.rs
    src/parser/event.rs
    src/parser/mod.rs
    src/source.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    std
)

PEERDIR(
    contrib/crates/winnow-1.0.4
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.5+spec-1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/toml_parser/toml_parser-1.1.5+spec-1.1.0.crate)

END()
