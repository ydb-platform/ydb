# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.2.19)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.2.19)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/bounds.rs
    ../src/cast.rs
    ../src/float.rs
    ../src/identities.rs
    ../src/int.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/ops/bytes.rs
    ../src/ops/checked.rs
    ../src/ops/euclid.rs
    ../src/ops/inv.rs
    ../src/ops/mod.rs
    ../src/ops/mul_add.rs
    ../src/ops/overflowing.rs
    ../src/ops/saturating.rs
    ../src/ops/wrapping.rs
    ../src/pow.rs
    ../src/real.rs
    ../src/sign.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
    ../RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    i128
    std
)
PEERDIR(
    contrib/crates/autocfg-1.5.1
)

CRATE_SOURCES(
    ../src/bounds.rs
    ../src/cast.rs
    ../src/float.rs
    ../src/identities.rs
    ../src/int.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/ops/bytes.rs
    ../src/ops/checked.rs
    ../src/ops/euclid.rs
    ../src/ops/inv.rs
    ../src/ops/mod.rs
    ../src/ops/mul_add.rs
    ../src/ops/overflowing.rs
    ../src/ops/saturating.rs
    ../src/ops/wrapping.rs
    ../src/pow.rs
    ../src/real.rs
    ../src/sign.rs
)

END()
