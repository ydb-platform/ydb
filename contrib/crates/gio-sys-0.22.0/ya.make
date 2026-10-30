# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(gio_sys)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(
    build.rs
    src/manual.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    Gir.toml
    LICENSE
    versions.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_ENV(
    SYSTEM_DEPS_GIO_2_0_NO_PKG_CONFIG=1
    SYSTEM_DEPS_GIO_2_0_LIB=gio_2_0
)

IF (OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_DARWIN AND ARCH_ARM64)
    CRATE_BUILD_SCRIPT(contrib/crates/gio-sys-0.22.0/build_script)
    PEERDIR(
    contrib/crates/glib-sys-0.22.0
    contrib/crates/gobject-sys-0.22.0
    contrib/crates/libc-0.2.191
)
ELSEIF (OS_WINDOWS AND ARCH_X86_64)
    CRATE_BUILD_SCRIPT(contrib/crates/gio-sys-0.22.0/build_script)
    PEERDIR(
    contrib/crates/glib-sys-0.22.0
    contrib/crates/gobject-sys-0.22.0
    contrib/crates/libc-0.2.191
    contrib/crates/windows-sys-0.61.2
)
ENDIF()

END()

RECURSE(build_script)
