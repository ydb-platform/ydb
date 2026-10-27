# Native Rust build with dependencies shared through contrib/crates.

CRATE(openssl)

CRATE_VERSION(0.10.81)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/aes.rs
    src/asn1.rs
    src/base64.rs
    src/bio.rs
    src/bn.rs
    src/cipher.rs
    src/cipher_ctx.rs
    src/cms.rs
    src/conf.rs
    src/derive.rs
    src/dh.rs
    src/dsa.rs
    src/ec.rs
    src/ecdsa.rs
    src/encrypt.rs
    src/envelope.rs
    src/error.rs
    src/ex_data.rs
    src/fips.rs
    src/hash.rs
    src/kdf.rs
    src/lib_ctx.rs
    src/macros.rs
    src/md.rs
    src/md_ctx.rs
    src/memcmp.rs
    src/nid.rs
    src/ocsp.rs
    src/ossl_param.rs
    src/pkcs12.rs
    src/pkcs5.rs
    src/pkcs7.rs
    src/pkey.rs
    src/pkey_ctx.rs
    src/provider.rs
    src/rand.rs
    src/rsa.rs
    src/sha.rs
    src/sign.rs
    src/srtp.rs
    src/ssl/bio.rs
    src/ssl/callbacks.rs
    src/ssl/connector.rs
    src/ssl/error.rs
    src/ssl/mod.rs
    src/ssl/test/mod.rs
    src/ssl/test/server.rs
    src/stack.rs
    src/string.rs
    src/symm.rs
    src/util.rs
    src/version.rs
    src/x509/extension.rs
    src/x509/mod.rs
    src/x509/store.rs
    src/x509/tests.rs
    src/x509/verify.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    LICENSE-APACHE
    README.md
    test/aia_bad_utf8_cert.pem
    test/aia_test_cert.pem
    test/alt_name_cert.pem
    test/authority_key_identifier.pem
    test/cert.pem
    test/certs.pem
    test/certv3.pem
    test/cms.p12
    test/cms_pubkey.der
    test/corrupted-rsa.pem
    test/crl-ca.crt
    test/csr.pem
    test/dhparams.pem
    test/entry_extensions.crl
    test/identity.p12
    test/intermediate-ca.pem
    test/key.der
    test/key.der.pub
    test/key.pem
    test/key.pem.pub
    test/keystore-empty-chain.p12
    test/leaf.pem
    test/nid_test_cert.pem
    test/nid_uid_test_cert.pem
    test/ocsp_ca_cert.der
    test/ocsp_resp_no_nextupdate.der
    test/ocsp_resp_revoked.der
    test/ocsp_subject_cert.der
    test/pkcs1.pem.pub
    test/pkcs8-nocrypt.der
    test/pkcs8.der
    test/root-ca.pem
    test/rsa-encrypted.pem
    test/rsa.pem
    test/rsa.pem.pub
    test/subca.crt
    test/test.crl
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    vendored
)

PEERDIR(
    contrib/crates/bitflags-2.13.2
    contrib/crates/cfg-if-1.0.5
    contrib/crates/foreign-types-0.3.2
    contrib/crates/libc-0.2.191
    contrib/crates/openssl-sys-0.9.117
)

CRATE_DEPENDENCY_ALIAS(ffi contrib/crates/openssl-sys-0.9.117)
CRATE_BUILD_SCRIPT(contrib/crates/openssl-0.10.81/build_script)

LICENSE("Apache-2.0")
VERSION(0.10.81)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl/openssl-0.10.81.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/openssl-macros-0.1.1
)

END()

RECURSE(build_script)
