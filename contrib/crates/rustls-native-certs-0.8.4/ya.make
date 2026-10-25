# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rustls_native_certs)

CRATE_VERSION(0.8.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/macos.rs
    src/unix.rs
    src/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
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
IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "x86_64-apple-darwin")
    PEERDIR(
        contrib/crates/security-framework-3.7.0
    )

ELSEIF (_RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/openssl-probe-0.2.1
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/schannel-0.1.29
    )

ENDIF()

LICENSE("Apache-2.0 OR ISC OR MIT")
VERSION(0.8.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustls-native-certs/rustls-native-certs-0.8.4.crate)

END()
