# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(toml)

CRATE_VERSION(1.1.3+spec-1.1.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/de/deserializer/array.rs
    src/de/deserializer/key.rs
    src/de/deserializer/mod.rs
    src/de/deserializer/table.rs
    src/de/deserializer/table_enum.rs
    src/de/deserializer/value.rs
    src/de/error.rs
    src/de/mod.rs
    src/de/parser/array.rs
    src/de/parser/dearray.rs
    src/de/parser/debug.rs
    src/de/parser/detable.rs
    src/de/parser/devalue.rs
    src/de/parser/document.rs
    src/de/parser/inline_table.rs
    src/de/parser/key.rs
    src/de/parser/mod.rs
    src/de/parser/value.rs
    src/macros.rs
    src/map.rs
    src/ser/document/array.rs
    src/ser/document/array_of_tables.rs
    src/ser/document/buffer.rs
    src/ser/document/map.rs
    src/ser/document/mod.rs
    src/ser/document/strategy.rs
    src/ser/error.rs
    src/ser/mod.rs
    src/ser/style.rs
    src/ser/value/array.rs
    src/ser/value/key.rs
    src/ser/value/map.rs
    src/ser/value/mod.rs
    src/table.rs
    src/value.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    display
    parse
    serde
    std
)

PEERDIR(
    contrib/crates/serde_core-1.0.229
    contrib/crates/serde_spanned-1.1.2
    contrib/crates/toml_datetime-1.1.1+spec-1.1.0
    contrib/crates/toml_parser-1.1.5+spec-1.1.0
    contrib/crates/toml_writer-1.1.3+spec-1.1.0
    contrib/crates/winnow-1.0.4
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.1.3+spec-1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/toml/toml-1.1.3+spec-1.1.0.crate)

END()
