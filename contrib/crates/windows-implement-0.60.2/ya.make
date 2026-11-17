# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(windows_implement)

CRATE_VERSION(0.60.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/gen.rs
    src/tests.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.60.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-implement/windows-implement-0.60.2.crate)

CRATE_SOURCES(
    src/gen.rs
    src/lib.rs
    src/tests.rs
)

END()
