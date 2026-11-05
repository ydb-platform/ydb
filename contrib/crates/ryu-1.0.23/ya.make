# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(ryu)

CRATE_VERSION(1.0.23)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/buffer/mod.rs
    src/common.rs
    src/d2s.rs
    src/d2s_full_table.rs
    src/d2s_intrinsics.rs
    src/d2s_small_table.rs
    src/digit_table.rs
    src/f2s.rs
    src/f2s_intrinsics.rs
    src/parse.rs
    src/pretty/exponent.rs
    src/pretty/mantissa.rs
    src/pretty/mod.rs
    src/s2d.rs
    src/s2f.rs
    src/tests.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-BOOST
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0 OR BSL-1.0")
VERSION(1.0.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/ryu/ryu-1.0.23.crate)

CRATE_SOURCES(
    src/buffer/mod.rs
    src/common.rs
    src/d2s.rs
    src/d2s_full_table.rs
    src/d2s_intrinsics.rs
    src/d2s_small_table.rs
    src/digit_table.rs
    src/f2s.rs
    src/f2s_intrinsics.rs
    src/lib.rs
    src/parse.rs
    src/pretty/exponent.rs
    src/pretty/mantissa.rs
    src/pretty/mod.rs
    src/s2d.rs
    src/s2f.rs
    src/tests.rs
)

END()
