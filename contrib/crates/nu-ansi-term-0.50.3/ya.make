# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(nu_ansi_term)

CRATE_VERSION(0.50.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ansi.rs
    src/debug.rs
    src/difference.rs
    src/display.rs
    src/gradient.rs
    src/rgb.rs
    src/style.rs
    src/util.rs
    src/windows.rs
    src/write.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-sys-0.61.2
)

    CRATE_DEPENDENCY_ALIAS(windows contrib/crates/windows-sys-0.61.2)
ENDIF()

LICENSE("MIT")
VERSION(0.50.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/nu-ansi-term/nu-ansi-term-0.50.3.crate)

CRATE_SOURCES(
    src/ansi.rs
    src/debug.rs
    src/difference.rs
    src/display.rs
    src/gradient.rs
    src/lib.rs
    src/rgb.rs
    src/style.rs
    src/util.rs
    src/windows.rs
    src/write.rs
)

END()
