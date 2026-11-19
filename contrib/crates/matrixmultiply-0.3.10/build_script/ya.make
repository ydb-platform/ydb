# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.3.10)
CRATE_EDITION(2018)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.3.10)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../examples/benchmark.rs
    ../examples/usegemm.rs
    ../spare_kernels/aarch64_neon_4x4.rs
    ../spare_kernels/x86_sse_sgemm.rs
    ../src/aarch64/macros.rs
    ../src/aarch64/mod.rs
    ../src/aligned_alloc.rs
    ../src/archmacros.rs
    ../src/archparam.rs
    ../src/archparam_defaults.rs
    ../src/cgemm_common.rs
    ../src/cgemm_kernel.rs
    ../src/constparse.rs
    ../src/debugmacros.rs
    ../src/dgemm_kernel.rs
    ../src/gemm.rs
    ../src/kernel.rs
    ../src/lib.rs
    ../src/loopmacros.rs
    ../src/packing.rs
    ../src/ptr.rs
    ../src/sgemm_kernel.rs
    ../src/threading.rs
    ../src/util.rs
    ../src/x86/macros.rs
    ../src/x86/mod.rs
    ../src/zgemm_kernel.rs
    ../testdefs/testdefs.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.rst
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)
PEERDIR(
    contrib/crates/autocfg-1.5.1
)

CRATE_FEATURES(
    cgemm
)

CRATE_SOURCES(
    ../testdefs/testdefs.rs
)

CRATE_SOURCES(
    ../src/aarch64/macros.rs
    ../src/aarch64/mod.rs
    ../src/aligned_alloc.rs
    ../src/archmacros.rs
    ../src/archparam.rs
    ../src/archparam_defaults.rs
    ../src/cgemm_common.rs
    ../src/cgemm_kernel.rs
    ../src/constparse.rs
    ../src/debugmacros.rs
    ../src/dgemm_kernel.rs
    ../src/gemm.rs
    ../src/kernel.rs
    ../src/lib.rs
    ../src/loopmacros.rs
    ../src/packing.rs
    ../src/ptr.rs
    ../src/sgemm_kernel.rs
    ../src/threading.rs
    ../src/util.rs
    ../src/x86/macros.rs
    ../src/x86/mod.rs
    ../src/zgemm_kernel.rs
)

END()
