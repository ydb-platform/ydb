# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/gobject.rs
    ../src/lib.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_ENV(
    SYSTEM_DEPS_CAIRO_NO_PKG_CONFIG=1
    SYSTEM_DEPS_CAIRO_LIB=cairo
    SYSTEM_DEPS_CAIRO_GOBJECT_NO_PKG_CONFIG=1
    SYSTEM_DEPS_CAIRO_GOBJECT_LIB=cairo-gobject
)

CRATE_FEATURES(
    glib-sys
    pdf
    png
    ps
    svg
    use_glib
    v1_16
)
PEERDIR(
    contrib/crates/system-deps-7.0.7
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
