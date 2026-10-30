# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(pin_project_internal)

CRATE_VERSION(1.1.13)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/pin_project/args.rs
    src/pin_project/attribute.rs
    src/pin_project/derive.rs
    src/pin_project/mod.rs
    src/pinned_drop.rs
    src/utils.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.1.13)
ORIGINAL_SOURCE(https://static.crates.io/crates/pin-project-internal/pin-project-internal-1.1.13.crate)

CRATE_SOURCES(
    src/error.rs
    src/lib.rs
    src/pin_project/args.rs
    src/pin_project/attribute.rs
    src/pin_project/derive.rs
    src/pin_project/mod.rs
    src/pinned_drop.rs
    src/utils.rs
)

END()
