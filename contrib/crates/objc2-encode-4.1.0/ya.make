# Native Rust build with dependencies shared through contrib/crates.

CRATE(objc2_encode)

CRATE_VERSION(4.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/encoding.rs
    src/encoding_box.rs
    src/helper.rs
    src/parse.rs
    src/static_str.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    std
)

LICENSE("MIT")
VERSION(4.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/objc2-encode/objc2-encode-4.1.0.crate)

END()
