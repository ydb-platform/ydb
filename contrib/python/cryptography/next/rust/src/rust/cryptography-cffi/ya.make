# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(cryptography_cffi)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/openssl-sys-0.9.117
    contrib/crates/pyo3-0.18.3
)

CRATE_PROC_MACROS(
    contrib/crates/indoc-1.0.9
    contrib/crates/pyo3-macros-0.18.3
    contrib/crates/serde_derive-1.0.229
)

LICENSE(Apache-2.0 OR BSD-3-Clause)
VERSION(0.1.0)

END()
