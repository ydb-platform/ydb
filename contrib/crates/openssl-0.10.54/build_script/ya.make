# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.10.54)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../examples/mk_certs.rs
    ../src/aes.rs
    ../src/asn1.rs
    ../src/base64.rs
    ../src/bio.rs
    ../src/bn.rs
    ../src/cipher.rs
    ../src/cipher_ctx.rs
    ../src/cms.rs
    ../src/conf.rs
    ../src/derive.rs
    ../src/dh.rs
    ../src/dsa.rs
    ../src/ec.rs
    ../src/ecdsa.rs
    ../src/encrypt.rs
    ../src/envelope.rs
    ../src/error.rs
    ../src/ex_data.rs
    ../src/fips.rs
    ../src/hash.rs
    ../src/lib.rs
    ../src/lib_ctx.rs
    ../src/macros.rs
    ../src/md.rs
    ../src/md_ctx.rs
    ../src/memcmp.rs
    ../src/nid.rs
    ../src/ocsp.rs
    ../src/pkcs12.rs
    ../src/pkcs5.rs
    ../src/pkcs7.rs
    ../src/pkey.rs
    ../src/pkey_ctx.rs
    ../src/provider.rs
    ../src/rand.rs
    ../src/rsa.rs
    ../src/sha.rs
    ../src/sign.rs
    ../src/srtp.rs
    ../src/ssl/bio.rs
    ../src/ssl/callbacks.rs
    ../src/ssl/connector.rs
    ../src/ssl/error.rs
    ../src/ssl/mod.rs
    ../src/ssl/test/mod.rs
    ../src/ssl/test/server.rs
    ../src/stack.rs
    ../src/string.rs
    ../src/symm.rs
    ../src/util.rs
    ../src/version.rs
    ../src/x509/extension.rs
    ../src/x509/mod.rs
    ../src/x509/store.rs
    ../src/x509/tests.rs
    ../src/x509/verify.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../CHANGELOG.md
    ../Cargo.lock
    ../LICENSE
    ../README.md
)

CRATE_FEATURES(
    default
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0")
VERSION(0.10.54)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl/openssl-0.10.54.crate)

END()
