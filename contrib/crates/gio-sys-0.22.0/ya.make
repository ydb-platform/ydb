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

IF (_RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "aarch64-apple-darwin")
    CRATE_BUILD_SCRIPT(contrib/crates/gio-sys-0.22.0/build_script)
    PEERDIR(
        contrib/crates/glib-sys-0.22.0
        contrib/crates/gobject-sys-0.22.0
        contrib/crates/libc-0.2.189
    )
ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    CRATE_BUILD_SCRIPT(contrib/crates/gio-sys-0.22.0/build_script)
    PEERDIR(
        contrib/crates/glib-sys-0.22.0
        contrib/crates/gobject-sys-0.22.0
        contrib/crates/libc-0.2.189
        contrib/crates/windows-sys-0.61.2
    )
ENDIF()

END()

RECURSE(build_script)
