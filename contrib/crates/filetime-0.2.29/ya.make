# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(filetime)

CRATE_VERSION(0.2.29)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/redox.rs
    src/unix/android.rs
    src/unix/linux.rs
    src/unix/macos.rs
    src/unix/mod.rs
    src/unix/utimensat.rs
    src/unix/utimes.rs
    src/wasm.rs
    src/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.29)
ORIGINAL_SOURCE(https://static.crates.io/crates/filetime/filetime-0.2.29.crate)

CRATE_SOURCES(
    src/redox.rs
    src/unix/mod.rs
    src/wasm.rs
    src/windows.rs
)

CRATE_SOURCES(
    src/lib.rs
    src/unix/android.rs
    src/unix/linux.rs
    src/unix/macos.rs
    src/unix/utimensat.rs
    src/unix/utimes.rs
)

END()
