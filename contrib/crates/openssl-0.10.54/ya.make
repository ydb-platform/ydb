# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(openssl)

CRATE_VERSION(0.10.54)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    examples/mk_certs.rs
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
    src/lib_ctx.rs
    src/macros.rs
    src/md.rs
    src/md_ctx.rs
    src/memcmp.rs
    src/nid.rs
    src/ocsp.rs
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FEATURES(
    default
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bitflags-1.3.2
    contrib/crates/cfg-if-1.0.4
    contrib/crates/foreign-types-0.3.2
    contrib/crates/libc-0.2.189
    contrib/crates/once_cell-1.21.4
    contrib/crates/openssl-sys-0.9.88
)

CRATE_DEPENDENCY_ALIAS(ffi contrib/crates/openssl-sys-0.9.88)
CRATE_BUILD_SCRIPT(contrib/crates/openssl-0.10.54/build_script)

LICENSE("Apache-2.0")
VERSION(0.10.54)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl/openssl-0.10.54.crate)

CRATE_PROC_MACROS(
    contrib/crates/openssl-macros-0.1.1
)

END()

RECURSE(build_script)
