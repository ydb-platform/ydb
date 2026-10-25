# Native Rust build with dependencies shared through contrib/crates.

CRATE(protobuf_support)

CRATE_VERSION(3.7.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/json_name.rs
    src/lexer/float.rs
    src/lexer/int.rs
    src/lexer/json_number_lit.rs
    src/lexer/lexer_impl.rs
    src/lexer/loc.rs
    src/lexer/mod.rs
    src/lexer/num_lit.rs
    src/lexer/parser_language.rs
    src/lexer/str_lit.rs
    src/lexer/token.rs
    src/lexer/tokenizer.rs
    src/text_format.rs
    src/toposort.rs
)

CRATE_INPUTS(
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/thiserror-1.0.69
)

LICENSE("MIT")
VERSION(3.7.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/protobuf-support/protobuf-support-3.7.2.crate)

CRATE_PROC_MACROS(
    contrib/crates/thiserror-impl-1.0.69
)

END()
