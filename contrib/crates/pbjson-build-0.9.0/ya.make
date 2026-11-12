# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pbjson_build)

CRATE_VERSION(0.9.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/descriptor.rs
    src/escape.rs
    src/generator.rs
    src/generator/enumeration.rs
    src/generator/message.rs
    src/message.rs
    src/resolver.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/heck-0.5.0
    contrib/crates/itertools-0.14.0
    contrib/crates/prost-0.14.4
    contrib/crates/prost-types-0.14.4
)

LICENSE("MIT")
VERSION(0.9.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pbjson-build/pbjson-build-0.9.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
