# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rustls_pki_types)

CRATE_VERSION(1.15.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/alg_id.rs
    src/base64.rs
    src/pem.rs
    src/server_name.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    src/data/README.md
    src/data/alg-ecdsa-p256.der
    src/data/alg-ecdsa-p256k1.der
    src/data/alg-ecdsa-p384.der
    src/data/alg-ecdsa-p521.der
    src/data/alg-ecdsa-sha256.der
    src/data/alg-ecdsa-sha384.der
    src/data/alg-ecdsa-sha512.der
    src/data/alg-ed25519.der
    src/data/alg-ed448.der
    src/data/alg-ml-dsa-44.der
    src/data/alg-ml-dsa-65.der
    src/data/alg-ml-dsa-87.der
    src/data/alg-rsa-encryption.der
    src/data/alg-rsa-pkcs1-sha256.der
    src/data/alg-rsa-pkcs1-sha384.der
    src/data/alg-rsa-pkcs1-sha512.der
    src/data/alg-rsa-pss-sha256.der
    src/data/alg-rsa-pss-sha384.der
    src/data/alg-rsa-pss-sha512.der
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
)

PEERDIR(
    contrib/crates/zeroize-1.9.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.15.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustls-pki-types/rustls-pki-types-1.15.1.crate)

CRATE_INPUTS(
    src/data/alg-ecdsa-p256.der
    src/data/alg-ecdsa-p256k1.der
    src/data/alg-ecdsa-p384.der
    src/data/alg-ecdsa-p521.der
    src/data/alg-ecdsa-sha256.der
    src/data/alg-ecdsa-sha384.der
    src/data/alg-ecdsa-sha512.der
    src/data/alg-ed25519.der
    src/data/alg-ed448.der
    src/data/alg-ml-dsa-44.der
    src/data/alg-ml-dsa-65.der
    src/data/alg-ml-dsa-87.der
    src/data/alg-rsa-encryption.der
    src/data/alg-rsa-pkcs1-sha256.der
    src/data/alg-rsa-pkcs1-sha384.der
    src/data/alg-rsa-pkcs1-sha512.der
    src/data/alg-rsa-pss-sha256.der
    src/data/alg-rsa-pss-sha384.der
    src/data/alg-rsa-pss-sha512.der
)

CRATE_SOURCES(
    src/alg_id.rs
    src/base64.rs
    src/lib.rs
    src/pem.rs
    src/server_name.rs
)

CRATE_INPUTS(
    src/data/README.md
)

END()
