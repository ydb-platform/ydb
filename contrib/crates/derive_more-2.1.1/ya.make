# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(derive_more)

CRATE_VERSION(2.1.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(2.1.1)
LICENSE("MIT")

CRATE_SOURCES(
    examples/deny_missing_docs.rs
    src/add.rs
    src/as.rs
    src/as_dyn_error.rs
    src/cmp.rs
    src/convert.rs
    src/fmt.rs
    src/ops.rs
    src/str.rs
    src/try_unwrap.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    add
    add_assign
    default
    std
)

CRATE_FEATURES(
    constructor
    debug
    display
    from
    into
    try_into
)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/add.rs
    src/as.rs
    src/as_dyn_error.rs
    src/cmp.rs
    src/convert.rs
    src/fmt.rs
    src/lib.rs
    src/ops.rs
    src/str.rs
    src/try_unwrap.rs
)

CRATE_PROC_MACROS(
    contrib/crates/derive_more-impl-2.1.1
)

END()
