# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(cairo_sys)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(
    build.rs
    src/gobject.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
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
CRATE_BUILD_SCRIPT(contrib/crates/cairo-sys-rs-0.22.0/build_script)
PEERDIR(
    contrib/crates/glib-sys-0.22.0
    contrib/crates/libc-0.2.191
)

END()

RECURSE(build_script)
