# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tempfile)

CRATE_VERSION(3.27.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/dir/imp/any.rs
    src/dir/imp/mod.rs
    src/dir/imp/unix.rs
    src/dir/mod.rs
    src/env.rs
    src/error.rs
    src/file/imp/mod.rs
    src/file/imp/other.rs
    src/file/imp/unix.rs
    src/file/imp/windows.rs
    src/file/mod.rs
    src/spooled.rs
    src/util.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    getrandom
)

PEERDIR(
    contrib/crates/fastrand-2.5.0
    contrib/crates/getrandom-0.4.3
    contrib/crates/once_cell-1.21.4
    contrib/crates/rustix-1.1.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(3.27.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/tempfile/tempfile-3.27.0.crate)

IF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-sys-0.61.2
)
ENDIF()

CRATE_SOURCES(
    src/dir/imp/any.rs
    src/dir/imp/mod.rs
    src/dir/imp/unix.rs
    src/dir/mod.rs
    src/env.rs
    src/error.rs
    src/file/imp/mod.rs
    src/file/imp/other.rs
    src/file/imp/unix.rs
    src/file/imp/windows.rs
    src/file/mod.rs
    src/lib.rs
    src/spooled.rs
    src/util.rs
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
