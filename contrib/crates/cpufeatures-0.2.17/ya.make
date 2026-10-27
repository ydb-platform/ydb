# Native Rust build with dependencies shared through contrib/crates.

CRATE(cpufeatures)

CRATE_VERSION(0.2.17)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/aarch64.rs
    src/loongarch64.rs
    src/miri.rs
    src/x86.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_DARWIN AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.17)
ORIGINAL_SOURCE(https://static.crates.io/crates/cpufeatures/cpufeatures-0.2.17.crate)

END()
