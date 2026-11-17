# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(locale_config)

CRATE_VERSION(0.3.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.0)
LICENSE("MIT")

CRATE_SOURCES(
    examples/show-user-locale.rs
    src/cgi.rs
    src/emscripten.rs
    src/macos.rs
    src/unix.rs
    src/win32.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


IF (OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/lazy_static-1.5.1
    contrib/crates/regex-1.13.1
)
ELSEIF (OS_DARWIN AND ARCH_X86_64 OR OS_DARWIN AND ARCH_ARM64)
    PEERDIR(
    contrib/crates/lazy_static-1.5.1
    contrib/crates/objc-0.2.7
    contrib/crates/objc-foundation-0.1.1
    contrib/crates/regex-1.13.1
)
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/lazy_static-1.5.1
    contrib/crates/regex-1.13.1
    contrib/crates/winapi-0.3.9
)
ENDIF()

END()
