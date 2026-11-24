# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(gdk_pixbuf_sys)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(build.rs)

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
    SYSTEM_DEPS_GDK_PIXBUF_2_0_NO_PKG_CONFIG=1
    SYSTEM_DEPS_GDK_PIXBUF_2_0_LIB=gdk_pixbuf_2_0
)

CRATE_BUILD_SCRIPT(contrib/crates/gdk-pixbuf-sys-0.22.0/build_script)
PEERDIR(
    contrib/crates/gio-sys-0.22.0
    contrib/crates/glib-sys-0.22.0
    contrib/crates/gobject-sys-0.22.0
    contrib/crates/libc-0.2.191
)

END()

RECURSE(build_script)
