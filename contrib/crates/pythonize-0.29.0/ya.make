# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pythonize)

CRATE_VERSION(0.29.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/de.rs
    src/error.rs
    src/ser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/pyo3-0.29.2
    contrib/crates/serde-1.0.229
)

LICENSE("MIT")
VERSION(0.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/pythonize/pythonize-0.29.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/pyo3-macros-0.29.2
    contrib/crates/serde_derive-1.0.229
)

END()
