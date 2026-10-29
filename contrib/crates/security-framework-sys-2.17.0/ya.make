# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(security_framework_sys)

CRATE_VERSION(2.17.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/access.rs
    src/access_control.rs
    src/authorization.rs
    src/base.rs
    src/certificate.rs
    src/certificate_oids.rs
    src/cipher_suite.rs
    src/cms.rs
    src/code_signing.rs
    src/digest_transform.rs
    src/encrypt_transform.rs
    src/identity.rs
    src/import_export.rs
    src/item.rs
    src/key.rs
    src/keychain.rs
    src/keychain_item.rs
    src/policy.rs
    src/random.rs
    src/secure_transport.rs
    src/transform.rs
    src/trust.rs
    src/trust_settings.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    OSX_10_13
    default
)

PEERDIR(
    contrib/crates/core-foundation-sys-0.8.7
    contrib/crates/libc-0.2.191
)

LICENSE("MIT OR Apache-2.0")
VERSION(2.17.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/security-framework-sys/security-framework-sys-2.17.0.crate)

CRATE_FEATURES(
    OSX_10_10
    OSX_10_11
    OSX_10_12
    OSX_10_9
)

CRATE_SOURCES(
    src/access.rs
    src/access_control.rs
    src/authorization.rs
    src/base.rs
    src/certificate.rs
    src/certificate_oids.rs
    src/cipher_suite.rs
    src/cms.rs
    src/code_signing.rs
    src/digest_transform.rs
    src/encrypt_transform.rs
    src/identity.rs
    src/import_export.rs
    src/item.rs
    src/key.rs
    src/keychain.rs
    src/keychain_item.rs
    src/lib.rs
    src/policy.rs
    src/random.rs
    src/secure_transport.rs
    src/transform.rs
    src/trust.rs
    src/trust_settings.rs
)

END()
