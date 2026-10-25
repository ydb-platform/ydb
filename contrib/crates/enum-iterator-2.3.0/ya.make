# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(enum_iterator)

CRATE_VERSION(2.3.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("0BSD")
VERSION(2.3.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/enum-iterator/enum-iterator-2.3.0.crate)

CRATE_SOURCES(
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/enum-iterator-derive-1.5.0
)

END()
