# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(pangocairo)

CRATE_VERSION(0.22.0)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.22.0)
LICENSE("MIT")

CRATE_SOURCES(
    src/auto/font.rs
    src/auto/font_map.rs
    src/auto/functions.rs
    src/auto/mod.rs
    src/font_map.rs
    src/prelude.rs
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
    contrib/crates/cairo-rs-0.22.0
    contrib/crates/glib-0.22.0
    contrib/crates/libc-0.2.191
    contrib/crates/pango-0.22.0
    contrib/crates/pangocairo-sys-0.22.0
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/futures-macro-0.3.34
    contrib/crates/glib-macros-0.22.0
    contrib/crates/serde_derive-1.0.229
)

END()
