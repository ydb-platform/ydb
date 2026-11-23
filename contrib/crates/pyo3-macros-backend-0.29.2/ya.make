# Native Rust build with dependencies shared through contrib/crates.

CRATE(pyo3_macros_backend)

CRATE_VERSION(0.29.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/attributes.rs
    src/combine_errors.rs
    src/derive_attributes.rs
    src/frompyobject.rs
    src/intopyobject.rs
    src/introspection.rs
    src/konst.rs
    src/method.rs
    src/module.rs
    src/params.rs
    src/py_expr.rs
    src/pyclass.rs
    src/pyfunction.rs
    src/pyfunction/signature.rs
    src/pyimpl.rs
    src/pymethod.rs
    src/quotes.rs
    src/utils.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/heck-0.5.0
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.29.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/pyo3-macros-backend/pyo3-macros-backend-0.29.2.crate)

END()
