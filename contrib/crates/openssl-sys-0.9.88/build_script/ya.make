# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_main)

CRATE_VERSION(0.9.88)
CRATE_EDITION(2018)
CRATE_ROOT(../build/main.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../build/cfgs.rs
    ../build/find_normal.rs
    ../build/find_vendored.rs
    ../build/run_bindgen.rs
    ../src/aes.rs
    ../src/asn1.rs
    ../src/bio.rs
    ../src/bn.rs
    ../src/cms.rs
    ../src/crypto.rs
    ../src/dtls1.rs
    ../src/ec.rs
    ../src/err.rs
    ../src/evp.rs
    ../src/handwritten/aes.rs
    ../src/handwritten/asn1.rs
    ../src/handwritten/bio.rs
    ../src/handwritten/bn.rs
    ../src/handwritten/cmac.rs
    ../src/handwritten/cms.rs
    ../src/handwritten/conf.rs
    ../src/handwritten/crypto.rs
    ../src/handwritten/dh.rs
    ../src/handwritten/dsa.rs
    ../src/handwritten/ec.rs
    ../src/handwritten/err.rs
    ../src/handwritten/evp.rs
    ../src/handwritten/hmac.rs
    ../src/handwritten/kdf.rs
    ../src/handwritten/mod.rs
    ../src/handwritten/object.rs
    ../src/handwritten/ocsp.rs
    ../src/handwritten/pem.rs
    ../src/handwritten/pkcs12.rs
    ../src/handwritten/pkcs7.rs
    ../src/handwritten/provider.rs
    ../src/handwritten/rand.rs
    ../src/handwritten/rsa.rs
    ../src/handwritten/safestack.rs
    ../src/handwritten/sha.rs
    ../src/handwritten/srtp.rs
    ../src/handwritten/ssl.rs
    ../src/handwritten/stack.rs
    ../src/handwritten/tls1.rs
    ../src/handwritten/types.rs
    ../src/handwritten/x509.rs
    ../src/handwritten/x509_vfy.rs
    ../src/handwritten/x509v3.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/obj_mac.rs
    ../src/ocsp.rs
    ../src/pem.rs
    ../src/pkcs7.rs
    ../src/rsa.rs
    ../src/sha.rs
    ../src/srtp.rs
    ../src/ssl.rs
    ../src/ssl3.rs
    ../src/tls1.rs
    ../src/types.rs
    ../src/x509.rs
    ../src/x509_vfy.rs
    ../src/x509v3.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../CHANGELOG.md
    ../LICENSE-MIT
    ../README.md
    ../build/expando.c
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cc-1.4.0
    contrib/crates/pkg-config-0.3.33
)

LICENSE("MIT")
VERSION(0.9.88)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl-sys/openssl-sys-0.9.88.crate)

END()
