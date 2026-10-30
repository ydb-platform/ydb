# Native Rust build with dependencies shared through contrib/crates.

CRATE(webpki)

CRATE_VERSION(0.103.16)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alg_tests.rs
    src/aws_lc_rs_algs.rs
    src/cert.rs
    src/crl/mod.rs
    src/crl/types.rs
    src/der.rs
    src/end_entity.rs
    src/error.rs
    src/ring_algs.rs
    src/rpk_entity.rs
    src/signed_data.rs
    src/subject_name/dns_name.rs
    src/subject_name/ip_address.rs
    src/subject_name/mod.rs
    src/time.rs
    src/trust_anchor.rs
    src/verify_cert.rs
    src/x509.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    src/data/alg-rsa-pkcs1-sha256-absent-params.der
    src/data/alg-rsa-pkcs1-sha384-absent-params.der
    src/data/alg-rsa-pkcs1-sha512-absent-params.der
    src/data/distrusted/EMDVTLSCAG3B.spki.der
    src/data/distrusted/EMDVTLSCAG3C.spki.der
    src/data/distrusted/EMEVTLSCAG3B.spki.der
    src/data/distrusted/EMEVTLSCAG3C.spki.der
    src/data/distrusted/EMOVCSCAG3B.spki.der
    src/data/distrusted/EMOVCSCAG3C.spki.der
    src/data/distrusted/EMOVTLSCAG3B.spki.der
    src/data/distrusted/EMOVTLSCAG3C.spki.der
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    ring
    std
)

PEERDIR(
    contrib/crates/ring-0.17.14
    contrib/crates/rustls-pki-types-1.15.1
    contrib/crates/untrusted-0.9.0
)

CRATE_DEPENDENCY_ALIAS(pki_types contrib/crates/rustls-pki-types-1.15.1)
LICENSE("ISC")
VERSION(0.103.16)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustls-webpki/rustls-webpki-0.103.16.crate)

END()
