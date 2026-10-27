# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(objc_foundation)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.1)
LICENSE("MIT")

CRATE_SOURCES(
    examples/custom_class.rs
    examples/example.rs
    src/array.rs
    src/data.rs
    src/dictionary.rs
    src/enumerator.rs
    src/macros.rs
    src/object.rs
    src/string.rs
    src/value.rs
)

CRATE_INPUTS(.cargo-checksum.json)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/block-0.1.6
    contrib/crates/objc-0.2.7
    contrib/crates/objc_id-0.1.1
)

END()
