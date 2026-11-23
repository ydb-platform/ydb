# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zerofrom)

CRATE_VERSION(0.1.8)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/macro_impls.rs
    src/zero_from.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    derive
)

LICENSE("Unicode-3.0")
VERSION(0.1.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerofrom/zerofrom-0.1.8.crate)

CRATE_FEATURES(
    alloc
)

CRATE_SOURCES(
    src/lib.rs
    src/macro_impls.rs
    src/zero_from.rs
)

CRATE_PROC_MACROS(
    contrib/crates/zerofrom-derive-0.1.8
)

END()
