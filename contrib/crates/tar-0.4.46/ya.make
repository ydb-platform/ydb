# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tar)

CRATE_VERSION(0.4.46)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/archive.rs
    src/builder.rs
    src/entry.rs
    src/entry_type.rs
    src/error.rs
    src/header.rs
    src/pax.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo/config.toml
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    xattr
)

PEERDIR(
    contrib/crates/filetime-0.2.29
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
    contrib/crates/xattr-1.6.1
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.46)
ORIGINAL_SOURCE(https://static.crates.io/crates/tar/tar-0.4.46.crate)

CRATE_SOURCES(
    src/archive.rs
    src/builder.rs
    src/entry.rs
    src/entry_type.rs
    src/error.rs
    src/header.rs
    src/lib.rs
    src/pax.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
