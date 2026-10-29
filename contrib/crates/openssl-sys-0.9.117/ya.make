# Native Rust build with dependencies shared through contrib/crates.

CRATE(openssl_sys)

CRATE_VERSION(0.9.117)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build/cfgs.rs
    build/find_normal.rs
    build/find_vendored.rs
    build/main.rs
    build/run_bindgen.rs
    src/aes.rs
    src/asn1.rs
    src/bio.rs
    src/bn.rs
    src/cms.rs
    src/core_dispatch.rs
    src/crypto.rs
    src/dh.rs
    src/dsa.rs
    src/dtls1.rs
    src/ec.rs
    src/err.rs
    src/evp.rs
    src/handwritten/aes.rs
    src/handwritten/asn1.rs
    src/handwritten/bio.rs
    src/handwritten/bn.rs
    src/handwritten/cmac.rs
    src/handwritten/cms.rs
    src/handwritten/conf.rs
    src/handwritten/crypto.rs
    src/handwritten/decoder.rs
    src/handwritten/dh.rs
    src/handwritten/dsa.rs
    src/handwritten/ec.rs
    src/handwritten/encoder.rs
    src/handwritten/err.rs
    src/handwritten/evp.rs
    src/handwritten/hmac.rs
    src/handwritten/kdf.rs
    src/handwritten/mod.rs
    src/handwritten/object.rs
    src/handwritten/ocsp.rs
    src/handwritten/params.rs
    src/handwritten/pem.rs
    src/handwritten/pkcs12.rs
    src/handwritten/pkcs7.rs
    src/handwritten/poly1305.rs
    src/handwritten/provider.rs
    src/handwritten/rand.rs
    src/handwritten/rsa.rs
    src/handwritten/safestack.rs
    src/handwritten/sha.rs
    src/handwritten/srtp.rs
    src/handwritten/ssl.rs
    src/handwritten/stack.rs
    src/handwritten/thread.rs
    src/handwritten/tls1.rs
    src/handwritten/types.rs
    src/handwritten/x509.rs
    src/handwritten/x509_vfy.rs
    src/handwritten/x509v3.rs
    src/macros.rs
    src/obj_mac.rs
    src/ocsp.rs
    src/pem.rs
    src/pkcs7.rs
    src/rsa.rs
    src/sha.rs
    src/srtp.rs
    src/ssl.rs
    src/ssl3.rs
    src/tls1.rs
    src/types.rs
    src/x509.rs
    src/x509_vfy.rs
    src/x509v3.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-MIT
    README.md
    build/expando.c
)

CRATE_FLAGS(--cap-lints=allow)



PEERDIR(
    contrib/crates/libc-0.2.191
)

CRATE_BUILD_SCRIPT(contrib/crates/openssl-sys-0.9.117/build_script)

LICENSE("MIT")
VERSION(0.9.117)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl-sys/openssl-sys-0.9.117.crate)

CRATE_ENV(OPENSSL_INCLUDE_DIR=${ARCADIA_ROOT}/contrib/libs/openssl/include)
CRATE_ENV(OPENSSL_LIB_DIR=${ARCADIA_BUILD_ROOT}/contrib/libs/openssl)
CRATE_ENV(OPENSSL_LIBS=)
CRATE_ENV(OPENSSL_STATIC=1)
CRATE_ENV(OPENSSL_NO_VENDOR=1)

IF (NOT OS_WINDOWS AND NOT EXPORT_CMAKE AND NOT OS_EMSCRIPTEN AND NOT MAPSMOBI_BUILD_TARGET)
    CRATE_FLAGS(--cfg=arcadia_openssl_renamed)
ENDIF()

PEERDIR(
    contrib/libs/openssl
)

CRATE_INPUTS(
    ${ARCADIA_ROOT}/contrib/libs/openssl/redef.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslconf-linux-aarch64.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslconf-linux.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslconf-osx-arm64.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslconf-osx.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslconf-win-x86_64.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslconf.h
    ${ARCADIA_ROOT}/contrib/libs/openssl/include/openssl/opensslv.h
)

END()

RECURSE(build_script)
