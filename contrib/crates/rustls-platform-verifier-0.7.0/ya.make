# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rustls_platform_verifier)

CRATE_VERSION(0.7.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/android.rs
    src/tests/ffi.rs
    src/tests/mod.rs
    src/tests/verification_mock/mod.rs
    src/tests/verification_real_world/mod.rs
    src/verification/android.rs
    src/verification/apple.rs
    src/verification/mod.rs
    src/verification/others.rs
    src/verification/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    src/tests/verification_mock/ca.go
    src/tests/verification_mock/go.mod
    src/tests/verification_mock/go.sum
    src/tests/verification_mock/root1-int1-ee_1-good.crt
    src/tests/verification_mock/root1-int1-ee_1-good.ocsp
    src/tests/verification_mock/root1-int1-ee_1-revoked.crt
    src/tests/verification_mock/root1-int1-ee_1-revoked.ocsp
    src/tests/verification_mock/root1-int1-ee_1-wrong_eku.crt
    src/tests/verification_mock/root1-int1-ee_127.0.0.1-good.crt
    src/tests/verification_mock/root1-int1-ee_127.0.0.1-good.ocsp
    src/tests/verification_mock/root1-int1-ee_127.0.0.1-revoked.crt
    src/tests/verification_mock/root1-int1-ee_127.0.0.1-revoked.ocsp
    src/tests/verification_mock/root1-int1-ee_127.0.0.1-wrong_eku.crt
    src/tests/verification_mock/root1-int1-ee_example.com-good.crt
    src/tests/verification_mock/root1-int1-ee_example.com-good.ocsp
    src/tests/verification_mock/root1-int1-ee_example.com-revoked.crt
    src/tests/verification_mock/root1-int1-ee_example.com-revoked.ocsp
    src/tests/verification_mock/root1-int1-ee_example.com-wrong_eku.crt
    src/tests/verification_mock/root1-int1.crt
    src/tests/verification_mock/root1.crt
    src/tests/verification_real_world/aws_amazon_com_valid_1.crt
    src/tests/verification_real_world/aws_amazon_com_valid_2.crt
    src/tests/verification_real_world/aws_amazon_com_valid_3.crt
    src/tests/verification_real_world/aws_amazon_com_valid_4.crt
    src/tests/verification_real_world/letsencrypt_org_valid_1.crt
    src/tests/verification_real_world/letsencrypt_org_valid_2.crt
    src/tests/verification_real_world/revoked_badssl_com_1.crt
    src/tests/verification_real_world/revoked_badssl_com_1.ocsp
    src/tests/verification_real_world/revoked_badssl_com_2.crt
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/log-0.4.33
    contrib/crates/rustls-0.23.42
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "x86_64-apple-darwin")
    PEERDIR(
        contrib/crates/core-foundation-0.10.1
        contrib/crates/core-foundation-sys-0.8.7
        contrib/crates/security-framework-3.7.0
        contrib/crates/security-framework-sys-2.17.0
    )

ELSEIF (_RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/rustls-native-certs-0.8.4
        contrib/crates/rustls-webpki-0.103.13
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/windows-sys-0.61.2
    )

ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.7.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustls-platform-verifier/rustls-platform-verifier-0.7.0.crate)

END()
