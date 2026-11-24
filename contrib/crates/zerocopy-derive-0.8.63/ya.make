# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(zerocopy_derive)

CRATE_VERSION(0.8.63)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/derive/from_bytes.rs
    src/derive/into_bytes.rs
    src/derive/known_layout.rs
    src/derive/mod.rs
    src/derive/project.rs
    src/derive/try_from_bytes.rs
    src/derive/unaligned.rs
    src/invariant.rs
    src/output_tests/expected/eq.expected.rs
    src/output_tests/expected/from_bytes_enum.expected.rs
    src/output_tests/expected/from_bytes_struct.expected.rs
    src/output_tests/expected/from_bytes_union.expected.rs
    src/output_tests/expected/from_zeros.expected.rs
    src/output_tests/expected/hash.expected.rs
    src/output_tests/expected/immutable.expected.rs
    "src/output_tests/expected/into_bytes_enum.repr(C).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(i128).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(i16).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(i32).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(i64).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(i8).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(isize).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(u128).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(u16).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(u32).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(u64).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(u8).expected.rs"
    "src/output_tests/expected/into_bytes_enum.repr(usize).expected.rs"
    src/output_tests/expected/into_bytes_struct_basic.expected.rs
    src/output_tests/expected/into_bytes_struct_empty.expected.rs
    src/output_tests/expected/into_bytes_struct_homogeneous_generic.expected.rs
    src/output_tests/expected/into_bytes_struct_trailing.expected.rs
    src/output_tests/expected/into_bytes_struct_trailing_generic.expected.rs
    src/output_tests/expected/known_layout_repr_c_struct.expected.rs
    src/output_tests/expected/known_layout_struct.expected.rs
    src/output_tests/expected/project_enum_1.expected.rs
    src/output_tests/expected/project_enum_2.expected.rs
    src/output_tests/expected/project_enum_3.expected.rs
    src/output_tests/expected/project_struct.expected.rs
    src/output_tests/expected/project_union_default_repr.expected.rs
    src/output_tests/expected/project_union_repr_c.expected.rs
    src/output_tests/expected/split_at_repr_c.expected.rs
    src/output_tests/expected/split_at_repr_c_packed.expected.rs
    src/output_tests/expected/split_at_repr_transparent.expected.rs
    src/output_tests/expected/try_from_bytes.expected.rs
    src/output_tests/expected/try_from_bytes_enum_1.expected.rs
    src/output_tests/expected/try_from_bytes_enum_2.expected.rs
    src/output_tests/expected/try_from_bytes_enum_3.expected.rs
    src/output_tests/expected/try_from_bytes_trivial_is_safe_enum.expected.rs
    src/output_tests/expected/unaligned.expected.rs
    src/output_tests/mod.rs
    src/repr.rs
    src/util.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-BSD
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("BSD-2-Clause OR Apache-2.0 OR MIT")
VERSION(0.8.63)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerocopy-derive/zerocopy-derive-0.8.63.crate)

END()
