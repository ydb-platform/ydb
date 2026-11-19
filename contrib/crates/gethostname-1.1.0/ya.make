# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(gethostname)

CRATE_VERSION(1.1.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
    deny.toml
    justfile
)

CRATE_FLAGS(--cap-lints=allow)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/rustix-1.1.5
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-link-0.2.1
)

ENDIF()

LICENSE("Apache-2.0")
VERSION(1.1.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/gethostname/gethostname-1.1.0.crate)

CRATE_SOURCES(
    src/lib.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
