# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(writeable)

CRATE_VERSION(0.6.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/cmp.rs
    src/concat.rs
    src/either.rs
    src/impls.rs
    src/ops.rs
    src/parts_write_adapter.rs
    src/testing.rs
    src/to_string_or_borrow.rs
    src/try_writeable.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Unicode-3.0")
VERSION(0.6.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/writeable/writeable-0.6.3.crate)

END()
