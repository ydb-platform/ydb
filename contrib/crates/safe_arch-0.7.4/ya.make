# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(safe_arch)

CRATE_VERSION(0.7.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.7.4)
LICENSE("Zlib OR Apache-2.0 OR MIT")

CRATE_SOURCES(
    src/naming_conventions.rs
    src/x86_x64/adx.rs
    src/x86_x64/aes.rs
    src/x86_x64/avx.rs
    src/x86_x64/avx2.rs
    src/x86_x64/bmi1.rs
    src/x86_x64/bmi2.rs
    src/x86_x64/fma.rs
    src/x86_x64/lzcnt.rs
    src/x86_x64/m128_.rs
    src/x86_x64/m128d_.rs
    src/x86_x64/m128i_.rs
    src/x86_x64/m256_.rs
    src/x86_x64/m256d_.rs
    src/x86_x64/m256i_.rs
    src/x86_x64/pclmulqdq.rs
    src/x86_x64/popcnt.rs
    src/x86_x64/rdrand.rs
    src/x86_x64/rdseed.rs
    src/x86_x64/sse.rs
    src/x86_x64/sse2.rs
    src/x86_x64/sse3.rs
    src/x86_x64/sse4_1.rs
    src/x86_x64/sse4_2.rs
    src/x86_x64/ssse3.rs
)

CRATE_INPUTS(
    .cargo/config.toml
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE.md
    LICENSE-MIT.md
    LICENSE-ZLIB.md
    README.md
    rustfmt.toml
    scripts/intel-helper.py
    tmp.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    bytemuck
    default
)
PEERDIR(
    contrib/crates/bytemuck-1.25.2
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
