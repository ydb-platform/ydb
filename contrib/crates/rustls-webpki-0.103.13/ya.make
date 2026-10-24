# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(webpki)

CRATE_VERSION(0.103.13)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
    src/data/alg-rsa-pkcs1-sha256-absent-params.der
    src/data/alg-rsa-pkcs1-sha384-absent-params.der
    src/data/alg-rsa-pkcs1-sha512-absent-params.der
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
VERSION(0.103.13)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustls-webpki/rustls-webpki-0.103.13.crate)

END()
