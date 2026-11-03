# Native Rust build with dependencies shared through contrib/crates.

CRATE(yoke)

CRATE_VERSION(0.8.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/cartable_ptr.rs
    src/either.rs
    src/erased.rs
    src/kinda_sorta_dangling.rs
    src/macro_impls.rs
    src/utils.rs
    src/yoke.rs
    src/yokeable.rs
    src/zero_from.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    derive
    zerofrom
)

PEERDIR(
    contrib/crates/stable_deref_trait-1.2.1
    contrib/crates/zerofrom-0.1.8
)

LICENSE("Unicode-3.0")
VERSION(0.8.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/yoke/yoke-0.8.4.crate)

CRATE_FEATURES(
    alloc
)

CRATE_PROC_MACROS(
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerofrom-derive-0.1.8
)

END()
