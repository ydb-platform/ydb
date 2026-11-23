# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(rgb)

CRATE_VERSION(0.8.52)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.8.52)
LICENSE("MIT")

CRATE_SOURCES(
    examples/example.rs
    examples/serde.rs
    src/as_bytes.rs
    src/bytemuck_impl.rs
    src/formats/abgr.rs
    src/formats/argb.rs
    src/formats/bgr.rs
    src/formats/bgra.rs
    src/formats/gray.rs
    src/formats/gray_a.rs
    src/formats/gray_alpha.rs
    src/formats/grb.rs
    src/formats/rgb.rs
    src/formats/rgba.rs
    src/inherent_impls.rs
    src/legacy/alt.rs
    src/legacy/internal/convert/array.rs
    src/legacy/internal/convert/mod.rs
    src/legacy/internal/convert/tuple.rs
    src/legacy/internal/ops.rs
    src/legacy/internal/pixel.rs
    src/legacy/internal/rgb.rs
    src/legacy/internal/rgba.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    argb
    as-bytes
    bytemuck
    default
    grb
)
PEERDIR(
    contrib/crates/bytemuck-1.25.2
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
