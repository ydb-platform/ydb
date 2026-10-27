# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(pyo3_macros_backend)

CRATE_VERSION(0.18.3)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/attributes.rs
    src/deprecations.rs
    src/frompyobject.rs
    src/konst.rs
    src/method.rs
    src/module.rs
    src/params.rs
    src/pyclass.rs
    src/pyfunction.rs
    src/pyfunction/signature.rs
    src/pyimpl.rs
    src/pymethod.rs
    src/utils.rs
)

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
    contrib/crates/quote-1.0.47
    contrib/crates/syn-1.0.109
)

LICENSE("Apache-2.0")
VERSION(0.18.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-macros-backend/pyo3-macros-backend-0.18.3.crate)

CRATE_SOURCES(
    src/attributes.rs
    src/deprecations.rs
    src/frompyobject.rs
    src/konst.rs
    src/lib.rs
    src/method.rs
    src/module.rs
    src/params.rs
    src/pyclass.rs
    src/pyfunction.rs
    src/pyfunction/signature.rs
    src/pyimpl.rs
    src/pymethod.rs
    src/utils.rs
)

END()
