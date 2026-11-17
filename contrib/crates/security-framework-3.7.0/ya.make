# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(security_framework)

CRATE_VERSION(3.7.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/access_control.rs
    src/authorization.rs
    src/base.rs
    src/certificate.rs
    src/cipher_suite.rs
    src/cms.rs
    src/identity.rs
    src/import_export.rs
    src/item.rs
    src/key.rs
    src/os/macos/access.rs
    src/os/macos/certificate.rs
    src/os/macos/certificate_oids.rs
    src/os/macos/code_signing.rs
    src/os/macos/digest_transform.rs
    src/os/macos/encrypt_transform.rs
    src/os/macos/identity.rs
    src/os/macos/import_export.rs
    src/os/macos/item.rs
    src/os/macos/key.rs
    src/os/macos/keychain.rs
    src/os/macos/keychain_item.rs
    src/os/macos/mod.rs
    src/os/macos/passwords.rs
    src/os/macos/secure_transport.rs
    src/os/macos/transform.rs
    src/os/mod.rs
    src/passwords.rs
    src/passwords_options.rs
    src/policy.rs
    src/random.rs
    src/secure_transport.rs
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
    THIRD_PARTY
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    OSX_10_14
    alpn
    default
    session-tickets
)

PEERDIR(
    contrib/crates/bitflags-2.13.2
    contrib/crates/core-foundation-0.10.1
    contrib/crates/core-foundation-sys-0.8.7
    contrib/crates/libc-0.2.191
    contrib/crates/security-framework-sys-2.17.0
)

LICENSE("MIT OR Apache-2.0")
VERSION(3.7.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/security-framework/security-framework-3.7.0.crate)

CRATE_FEATURES(
    OSX_10_12
)

CRATE_SOURCES(
    src/access_control.rs
    src/authorization.rs
    src/base.rs
    src/certificate.rs
    src/cipher_suite.rs
    src/cms.rs
    src/identity.rs
    src/import_export.rs
    src/item.rs
    src/key.rs
    src/lib.rs
    src/os/macos/access.rs
    src/os/macos/certificate.rs
    src/os/macos/certificate_oids.rs
    src/os/macos/code_signing.rs
    src/os/macos/digest_transform.rs
    src/os/macos/encrypt_transform.rs
    src/os/macos/identity.rs
    src/os/macos/import_export.rs
    src/os/macos/item.rs
    src/os/macos/key.rs
    src/os/macos/keychain.rs
    src/os/macos/keychain_item.rs
    src/os/macos/mod.rs
    src/os/macos/passwords.rs
    src/os/macos/secure_transport.rs
    src/os/macos/transform.rs
    src/os/mod.rs
    src/passwords.rs
    src/passwords_options.rs
    src/policy.rs
    src/random.rs
    src/secure_transport.rs
    src/trust.rs
    src/trust_settings.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
