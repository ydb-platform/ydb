# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(librsvg_c)

CRATE_VERSION(2.62.0)
CRATE_EDITION(2024)
CRATE_ROOT(librsvg-c/src/lib.rs)
CRATE_MANIFEST(librsvg-c/Cargo.toml)
CRATE_MANIFEST_DIR(librsvg-c)

CRATE_SOURCES(
    librsvg-c/build.rs
    librsvg-c/src/dpi.rs
    librsvg-c/src/handle.rs
    librsvg-c/src/messages.rs
    librsvg-c/src/pixbuf_utils.rs
    librsvg-c/src/sizing.rs
)

CRATE_INPUTS(
    librsvg-c/COPYING.LIB
    librsvg-c/meson.build
    meson.build
)


CRATE_FEATURES(pixbuf)
CRATE_BUILD_SCRIPT(contrib/libs/librsvg/librsvg-c/build_script)
PEERDIR(
    contrib/libs/librsvg/rsvg
    contrib/crates/cairo-rs-0.22.0
    contrib/crates/cast-0.3.0
    contrib/crates/float-cmp-0.10.0
    contrib/crates/gdk-pixbuf-0.22.0
    contrib/crates/gio-0.22.1
    contrib/crates/glib-0.22.0
    contrib/crates/libc-0.2.191
    contrib/crates/rgb-0.8.52
    contrib/crates/url-2.5.8
)
CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/cssparser-macros-0.6.1
    contrib/crates/derive_more-impl-2.1.1
    contrib/crates/displaydoc-0.2.7
    contrib/crates/futures-macro-0.3.34
    contrib/crates/glib-macros-0.22.0
    contrib/crates/multiversion_no_op-1.0.0
    contrib/crates/mutants-0.0.3
    contrib/crates/nalgebra-macros-0.3.0
    contrib/crates/paste-1.0.15
    contrib/crates/phf_macros-0.11.3
    contrib/crates/rustversion-1.0.23
    contrib/crates/serde_derive-1.0.229
    contrib/crates/yoke-derive-0.8.4
    contrib/crates/zerocopy-derive-0.8.63
    contrib/crates/zerofrom-derive-0.1.8
    contrib/crates/zerovec-derive-0.11.7
)

LICENSE(Apache-2.0 AND BSD-3-Clause AND LGPL-2.1-only AND LGPL-2.1-or-later AND MIT AND MPL-2.0)
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)
VERSION(2.62.0)
ORIGINAL_SOURCE(https://github.com/GNOME/librsvg/archive/refs/tags/2.62.0.tar.gz)

PEERDIR(
    contrib/libs/cairo
    contrib/libs/cairo/util/cairo-gobject
    contrib/libs/fontconfig
    contrib/libs/gdk-pixbuf
    contrib/libs/libcroco
    contrib/libs/libxml
    contrib/libs/pango
    contrib/libs/pango/pangocairo-1.0
    contrib/libs/pango/pangoft2-1.0
    contrib/restricted/glib
)

ADDINCL(
    GLOBAL contrib/libs/librsvg/include
    GLOBAL contrib/libs/librsvg/include/librsvg
    GLOBAL contrib/libs/cairo/src
    GLOBAL contrib/libs/gdk-pixbuf
)

END()

RECURSE_FOR_TESTS(ut)
