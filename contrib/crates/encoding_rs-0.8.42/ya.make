# Native Rust build with dependencies shared through contrib/crates.

CRATE(encoding_rs)

CRATE_VERSION(0.8.42)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/ascii.rs
    src/big5.rs
    src/data.rs
    src/euc_jp.rs
    src/euc_kr.rs
    src/gb18030.rs
    src/gb18030_2022.rs
    src/handles.rs
    src/iso_2022_jp.rs
    src/macros.rs
    src/mem.rs
    src/replacement.rs
    src/shift_jis.rs
    src/simd_funcs.rs
    src/single_byte.rs
    src/test_labels_names.rs
    src/testing.rs
    src/utf_16.rs
    src/utf_8.rs
    src/variant.rs
    src/x_user_defined.rs
)

CRATE_INPUTS(
    COPYRIGHT
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-WHATWG
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
)

PEERDIR(
    contrib/crates/cfg-if-1.0.5
    contrib/crates/scopeguard-1.2.0
    contrib/crates/simdutf8-0.1.5
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL)
ELSEIF (OS_DARWIN AND ARCH_X86_64 OR OS_WINDOWS AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/core_detect-1.0.0
)

ENDIF()

CRATE_BUILD_SCRIPT(contrib/crates/encoding_rs-0.8.42/build_script)

LICENSE("(Apache-2.0 OR MIT) AND BSD-3-Clause")
VERSION(0.8.42)
ORIGINAL_SOURCE(https://static.crates.io/crates/encoding_rs/encoding_rs-0.8.42.crate)

CRATE_PROC_MACROS(
    contrib/crates/multiversion_no_op-1.0.0
    contrib/crates/rustversion-1.0.23
)

END()

RECURSE(build_script)
