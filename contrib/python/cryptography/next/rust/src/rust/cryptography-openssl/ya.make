# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(cryptography_openssl)

CRATE_VERSION(0.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/fips.rs
    src/hmac.rs
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/foreign-types-0.3.2
    contrib/crates/foreign-types-shared-0.1.1
    contrib/crates/openssl-0.10.81
    contrib/crates/openssl-sys-0.9.117
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/openssl-macros-0.1.1
)

CRATE_DEPENDENCY_ALIAS(ffi contrib/crates/openssl-sys-0.9.117)
CRATE_BUILD_SCRIPT(contrib/python/cryptography/next/rust/src/rust/cryptography-openssl/build_script)

LICENSE(Apache-2.0 OR BSD-3-Clause)
VERSION(0.1.0)

END()
