# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(gdk_pixbuf)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(
    src/auto/enums.rs
    src/auto/flags.rs
    src/auto/mod.rs
    src/auto/pixbuf.rs
    src/auto/pixbuf_animation.rs
    src/auto/pixbuf_format.rs
    src/auto/pixbuf_loader.rs
    src/auto/pixbuf_non_anim.rs
    src/auto/pixbuf_simple_anim.rs
    src/pixbuf.rs
    src/pixbuf_animation.rs
    src/pixbuf_animation_iter.rs
    src/prelude.rs
    src/subclass/mod.rs
    src/subclass/pixbuf_animation.rs
    src/subclass/pixbuf_animation_iter.rs
    src/subclass/pixbuf_loader.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    COPYRIGHT
    Cargo.lock
    Gir.toml
    LICENSE
    README.md
    src/auto/versions.txt
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/gdk-pixbuf-sys-0.22.0
    contrib/crates/gio-0.22.1
    contrib/crates/glib-0.22.0
    contrib/crates/libc-0.2.191
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/futures-macro-0.3.34
    contrib/crates/glib-macros-0.22.0
    contrib/crates/serde_derive-1.0.229
)

END()
