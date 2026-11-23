# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(errno)

CRATE_VERSION(0.3.14)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/hermit.rs
    src/sys.rs
    src/unix.rs
    src/wasi.rs
    src/windows.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    clippy.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/libc-0.2.191
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.14)
ORIGINAL_SOURCE(https://static.crates.io/crates/errno/errno-0.3.14.crate)

IF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-sys-0.61.2
)
ENDIF()

CRATE_SOURCES(
    src/hermit.rs
    src/lib.rs
    src/sys.rs
    src/unix.rs
    src/wasi.rs
    src/windows.rs
)

END()
