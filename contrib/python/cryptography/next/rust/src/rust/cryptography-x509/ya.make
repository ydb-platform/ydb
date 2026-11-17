# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(cryptography_x509)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/certificate.rs
    src/common.rs
    src/crl.rs
    src/csr.rs
    src/extensions.rs
    src/name.rs
    src/ocsp_req.rs
    src/ocsp_resp.rs
    src/oid.rs
    src/pkcs7.rs
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/asn1-0.15.2
)

CRATE_PROC_MACROS(
    contrib/crates/asn1_derive-0.15.2
)

LICENSE(Apache-2.0 OR BSD-3-Clause)
VERSION(0.1.0)

END()
