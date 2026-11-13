# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(socket2)

CRATE_VERSION(0.6.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/sockaddr.rs
    src/socket.rs
    src/sockref.rs
    src/sys/unix.rs
    src/sys/windows.rs
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
    all
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-sys-0.61.2
)

ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.6.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/socket2/socket2-0.6.5.crate)

CRATE_SOURCES(
    src/lib.rs
    src/sockaddr.rs
    src/socket.rs
    src/sockref.rs
    src/sys/unix.rs
    src/sys/windows.rs
)

END()
