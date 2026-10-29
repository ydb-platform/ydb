# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(wide)

CRATE_VERSION(0.7.33)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.7.33)
LICENSE("Zlib OR Apache-2.0 OR MIT")

CRATE_SOURCES(
    src/f32x4_.rs
    src/f32x8_.rs
    src/f64x2_.rs
    src/f64x4_.rs
    src/i16x16_.rs
    src/i16x8_.rs
    src/i32x4_.rs
    src/i32x8_.rs
    src/i64x2_.rs
    src/i64x4_.rs
    src/i8x16_.rs
    src/i8x32_.rs
    src/macros.rs
    src/u16x16_.rs
    src/u16x8_.rs
    src/u32x4_.rs
    src/u32x8_.rs
    src/u64x2_.rs
    src/u64x4_.rs
    src/u8x16_.rs
    src/u8x32_.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo-ci/config
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-ZLIB.md
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(std)
PEERDIR(
    contrib/crates/bytemuck-1.25.2
    contrib/crates/safe_arch-0.7.4
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
