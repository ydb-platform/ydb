# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.8.42)
CRATE_EDITION(2024)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/ascii.rs
    ../src/big5.rs
    ../src/data.rs
    ../src/euc_jp.rs
    ../src/euc_kr.rs
    ../src/gb18030.rs
    ../src/gb18030_2022.rs
    ../src/handles.rs
    ../src/iso_2022_jp.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/mem.rs
    ../src/replacement.rs
    ../src/shift_jis.rs
    ../src/simd_funcs.rs
    ../src/single_byte.rs
    ../src/test_labels_names.rs
    ../src/testing.rs
    ../src/utf_16.rs
    ../src/utf_8.rs
    ../src/variant.rs
    ../src/x_user_defined.rs
)

CRATE_INPUTS(
    ../COPYRIGHT
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../LICENSE-WHATWG
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
)

LICENSE("(Apache-2.0 OR MIT) AND BSD-3-Clause")
VERSION(0.8.42)
ORIGINAL_SOURCE(https://static.crates.io/crates/encoding_rs/encoding_rs-0.8.42.crate)

CRATE_PROC_MACROS(
    contrib/crates/rustversion-1.0.23
)

END()
