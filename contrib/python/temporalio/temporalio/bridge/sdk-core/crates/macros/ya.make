# Native Rust build; versions and features follow temporalio 1.32.0 Cargo.lock.

CRATE_PROC_MACRO(temporalio_macros)

CRATE_VERSION(0.6.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/activities_definitions.rs
    src/fsm_impl.rs
    src/macro_utils.rs
    src/workflow_definitions.rs
)

CRATE_INPUTS(
    LICENSE.txt
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE(MIT)
VERSION(0.6.0)

END()
