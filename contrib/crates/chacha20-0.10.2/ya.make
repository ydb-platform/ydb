# Native Rust build with dependencies shared through contrib/crates.

CRATE(chacha20)

CRATE_VERSION(0.10.2)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/backends.rs
    src/backends/avx2.rs
    src/backends/avx512.rs
    src/backends/neon.rs
    src/backends/soft.rs
    src/backends/sse2.rs
    src/chacha.rs
    src/legacy.rs
    src/rng.rs
    src/variants.rs
    src/xchacha.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    rng
)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/rand_core-0.10.1
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL)
ELSEIF (OS_DARWIN AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/cpufeatures-0.3.1
)

ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/chacha20/chacha20-0.10.2.crate)

END()
