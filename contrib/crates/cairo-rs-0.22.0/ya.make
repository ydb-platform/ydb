# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(cairo)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(
    src/constants.rs
    src/context.rs
    src/device.rs
    src/enums.rs
    src/error.rs
    src/font/font_extents.rs
    src/font/font_face.rs
    src/font/font_options.rs
    src/font/glyph.rs
    src/font/mod.rs
    src/font/scaled_font.rs
    src/font/text_cluster.rs
    src/font/text_extents.rs
    src/font/user_fonts.rs
    src/image_surface.rs
    src/matrices.rs
    src/paths.rs
    src/patterns.rs
    src/pdf.rs
    src/ps.rs
    src/quartz_surface.rs
    src/recording_surface.rs
    src/rectangle.rs
    src/rectangle_int.rs
    src/region.rs
    src/stream.rs
    src/surface.rs
    src/surface_macros.rs
    src/surface_png.rs
    src/svg.rs
    src/user_data.rs
    src/utils.rs
    src/win32_surface.rs
    src/xcb.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    COPYRIGHT
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    glib
    pdf
    png
    ps
    svg
    use_glib
    v1_16
)
PEERDIR(
    contrib/crates/bitflags-2.13.2
    contrib/crates/cairo-sys-rs-0.22.0
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
