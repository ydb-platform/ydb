# Native Rust build with dependencies shared through contrib/crates.

CRATE(tinystr)

CRATE_VERSION(0.8.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ascii.rs
    src/asciibyte.rs
    src/databake.rs
    src/error.rs
    src/int_ops.rs
    src/macros.rs
    src/serde.rs
    src/ule.rs
    src/unvalidated.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    zerovec
)

PEERDIR(
    contrib/crates/zerovec-0.11.9
)

LICENSE("Unicode-3.0")
VERSION(0.8.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/tinystr/tinystr-0.8.5.crate)

CRATE_FEATURES(
    alloc
)

CRATE_PROC_MACROS(
    contrib/crates/displaydoc-0.2.7
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
