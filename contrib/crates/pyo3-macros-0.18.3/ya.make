# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE_PROC_MACRO(pyo3_macros)

CRATE_VERSION(0.18.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
)

CRATE_FEATURES(
    abi3
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/pyo3-macros-backend-0.18.3
    contrib/crates/quote-1.0.47
    contrib/crates/syn-1.0.109
)

LICENSE("Apache-2.0")
VERSION(0.18.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-macros/pyo3-macros-0.18.3.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
