# Native Rust build with dependencies shared through contrib/crates.

CRATE(rustls_native_certs)

CRATE_VERSION(0.8.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/macos.rs
    src/unix.rs
    src/windows.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    LICENSE-APACHE
    LICENSE-ISC
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/rustls-pki-types-1.15.1
)

CRATE_DEPENDENCY_ALIAS(pki_types contrib/crates/rustls-pki-types-1.15.1)
IF (OS_DARWIN AND ARCH_ARM64 OR OS_DARWIN AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/security-framework-3.7.0
)

ELSEIF (OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/openssl-probe-0.2.1
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/schannel-0.1.30
)

ENDIF()

LICENSE("Apache-2.0 OR ISC OR MIT")
VERSION(0.8.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustls-native-certs/rustls-native-certs-0.8.5.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
