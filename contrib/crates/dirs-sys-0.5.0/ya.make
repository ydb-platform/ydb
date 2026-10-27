# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(dirs_sys)

CRATE_VERSION(0.5.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/xdg_user_dirs.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CONTRIBUTING.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/option-ext-0.2.0
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
VERSION(0.5.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/dirs-sys/dirs-sys-0.5.0.crate)

CRATE_SOURCES(
    src/lib.rs
    src/xdg_user_dirs.rs
)

END()
