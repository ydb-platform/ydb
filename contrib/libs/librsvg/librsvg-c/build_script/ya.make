# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(2.62.0)
CRATE_EDITION(2024)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(2.62.0)
LICENSE("LGPL-2.1-or-later")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/dpi.rs
    ../src/handle.rs
    ../src/lib.rs
    ../src/messages.rs
    ../src/pixbuf_utils.rs
    ../src/sizing.rs
)

CRATE_INPUTS(
    ../COPYING.LIB
    ../meson.build
    ../../meson.build
)


CRATE_FEATURES(pixbuf)
PEERDIR(contrib/crates/regex-1.13.1)

END()
