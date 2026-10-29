# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.31.0)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.31.0)
LICENSE("MPL-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../attr.rs
    ../bloom.rs
    ../builder.rs
    ../context.rs
    ../kleene_value.rs
    ../lib.rs
    ../matching.rs
    ../nth_index_cache.rs
    ../parser.rs
    ../relative_selector/cache.rs
    ../relative_selector/filter.rs
    ../relative_selector/mod.rs
    ../sink.rs
    ../tree.rs
    ../visitor.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../CHANGES.md
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/phf_codegen-0.11.3
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
