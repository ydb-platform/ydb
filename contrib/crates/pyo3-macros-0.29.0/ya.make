# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(pyo3_macros)

CRATE_VERSION(0.29.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    multiple-pymethods
)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/pyo3-macros-backend-0.29.0
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-macros/pyo3-macros-0.29.0.crate)

END()
