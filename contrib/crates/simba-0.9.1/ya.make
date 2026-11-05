# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(simba)

CRATE_VERSION(0.9.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.9.1)
LICENSE("Apache-2.0")

CRATE_SOURCES(
    src/scalar/complex.rs
    src/scalar/field.rs
    src/scalar/fixed_impl.rs
    src/scalar/mod.rs
    src/scalar/real.rs
    src/scalar/subset.rs
    src/simd/auto_simd_impl.rs
    src/simd/mod.rs
    src/simd/portable_simd_impl.rs
    src/simd/rand_impl.rs
    src/simd/simd_bool.rs
    src/simd/simd_complex.rs
    src/simd/simd_option.rs
    src/simd/simd_partial_ord.rs
    src/simd/simd_real.rs
    src/simd/simd_signed.rs
    src/simd/simd_value.rs
    src/simd/wide_simd_impl.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG
    Cargo.lock
    LICENSE
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
    wide
)
PEERDIR(
    contrib/crates/approx-0.5.1
    contrib/crates/num-complex-0.4.6
    contrib/crates/num-traits-0.2.19
    contrib/crates/wide-0.7.33
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/paste-1.0.15
)

END()
