# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(cryptography_rust)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/asn1.rs
    src/backend/dh.rs
    src/backend/dsa.rs
    src/backend/ed25519.rs
    src/backend/ed448.rs
    src/backend/hashes.rs
    src/backend/hmac.rs
    src/backend/kdf.rs
    src/backend/mod.rs
    src/backend/poly1305.rs
    src/backend/utils.rs
    src/backend/x25519.rs
    src/backend/x448.rs
    src/buf.rs
    src/error.rs
    src/exceptions.rs
    src/oid.rs
    src/pkcs7.rs
    src/pool.rs
    src/x509/certificate.rs
    src/x509/common.rs
    src/x509/crl.rs
    src/x509/csr.rs
    src/x509/extensions.rs
    src/x509/mod.rs
    src/x509/ocsp.rs
    src/x509/ocsp_req.rs
    src/x509/ocsp_resp.rs
    src/x509/sct.rs
    src/x509/sign.rs
)

CRATE_INPUTS(
    Cargo.lock
)

CRATE_FEATURES(
    default
    extension-module
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/asn1-0.15.2
    contrib/crates/foreign-types-shared-0.1.1
    contrib/crates/once_cell-1.21.4
    contrib/crates/openssl-0.10.81
    contrib/crates/openssl-sys-0.9.117
    contrib/crates/ouroboros-0.15.6
    contrib/crates/pem-1.1.1
    contrib/crates/pyo3-0.18.3
    contrib/python/cryptography/next/rust/src/rust/cryptography-cffi
    contrib/python/cryptography/next/rust/src/rust/cryptography-openssl
    contrib/python/cryptography/next/rust/src/rust/cryptography-x509
)

CRATE_PROC_MACROS(
    contrib/crates/asn1_derive-0.15.2
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/indoc-1.0.9
    contrib/crates/openssl-macros-0.1.1
    contrib/crates/ouroboros_macro-0.15.6
    contrib/crates/proc-macro-error-attr-1.0.4
    contrib/crates/pyo3-macros-0.18.3
    contrib/crates/serde_derive-1.0.229
)

CRATE_BUILD_SCRIPT(contrib/python/cryptography/next/rust/src/rust/build_script)

LICENSE(Apache-2.0 OR BSD-3-Clause)
VERSION(0.1.0)

END()
