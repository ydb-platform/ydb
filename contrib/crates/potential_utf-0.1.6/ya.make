# Native Rust build with dependencies shared through contrib/crates.

CRATE(potential_utf)

CRATE_VERSION(0.1.6)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/uchar.rs
    src/ustr.rs
    src/writeable.rs
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
VERSION(0.1.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/potential_utf/potential_utf-0.1.6.crate)

CRATE_PROC_MACROS(
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

END()
