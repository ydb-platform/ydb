# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(matrixmultiply)

CRATE_VERSION(0.3.10)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.10)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    build.rs
    examples/benchmark.rs
    examples/usegemm.rs
    spare_kernels/aarch64_neon_4x4.rs
    spare_kernels/x86_sse_sgemm.rs
    src/aarch64/macros.rs
    src/aarch64/mod.rs
    src/aligned_alloc.rs
    src/archmacros.rs
    src/archparam.rs
    src/archparam_defaults.rs
    src/cgemm_common.rs
    src/cgemm_kernel.rs
    src/constparse.rs
    src/debugmacros.rs
    src/dgemm_kernel.rs
    src/gemm.rs
    src/kernel.rs
    src/loopmacros.rs
    src/packing.rs
    src/ptr.rs
    src/sgemm_kernel.rs
    src/threading.rs
    src/util.rs
    src/x86/macros.rs
    src/x86/mod.rs
    src/zgemm_kernel.rs
    testdefs/testdefs.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.rst
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_FEATURES(
    default
    std
)
CRATE_BUILD_SCRIPT(contrib/crates/matrixmultiply-0.3.10/build_script)
PEERDIR(
    contrib/crates/rawpointer-0.2.1
)

CRATE_FEATURES(
    cgemm
    num_cpus
    once_cell
    thread-tree
    threading
)

PEERDIR(
    contrib/crates/num_cpus-1.17.0
    contrib/crates/once_cell-1.21.4
    contrib/crates/thread-tree-0.3.3
)

CRATE_SOURCES(
    testdefs/testdefs.rs
)

CRATE_SOURCES(
    src/aarch64/macros.rs
    src/aarch64/mod.rs
    src/aligned_alloc.rs
    src/archmacros.rs
    src/archparam.rs
    src/archparam_defaults.rs
    src/cgemm_common.rs
    src/cgemm_kernel.rs
    src/constparse.rs
    src/debugmacros.rs
    src/dgemm_kernel.rs
    src/gemm.rs
    src/kernel.rs
    src/lib.rs
    src/loopmacros.rs
    src/packing.rs
    src/ptr.rs
    src/sgemm_kernel.rs
    src/threading.rs
    src/util.rs
    src/x86/macros.rs
    src/x86/mod.rs
    src/zgemm_kernel.rs
)

END()

RECURSE(build_script)
