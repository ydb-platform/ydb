# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/asn1.rs
    ../src/backend/dh.rs
    ../src/backend/dsa.rs
    ../src/backend/ed25519.rs
    ../src/backend/ed448.rs
    ../src/backend/hashes.rs
    ../src/backend/hmac.rs
    ../src/backend/kdf.rs
    ../src/backend/mod.rs
    ../src/backend/poly1305.rs
    ../src/backend/utils.rs
    ../src/backend/x25519.rs
    ../src/backend/x448.rs
    ../src/buf.rs
    ../src/error.rs
    ../src/exceptions.rs
    ../src/lib.rs
    ../src/oid.rs
    ../src/pkcs7.rs
    ../src/pool.rs
    ../src/x509/certificate.rs
    ../src/x509/common.rs
    ../src/x509/crl.rs
    ../src/x509/csr.rs
    ../src/x509/extensions.rs
    ../src/x509/mod.rs
    ../src/x509/ocsp.rs
    ../src/x509/ocsp_req.rs
    ../src/x509/ocsp_resp.rs
    ../src/x509/sct.rs
    ../src/x509/sign.rs
)

CRATE_INPUTS(
    ../Cargo.lock
)

CRATE_FEATURES(
    default
    extension-module
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cc-1.10.0
)

LICENSE(Apache-2.0 OR BSD-3-Clause)
VERSION(0.1.0)

END()
