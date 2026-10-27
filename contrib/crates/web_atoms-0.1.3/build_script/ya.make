# Native Rust build POC; host build script.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.1.3)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
VERSION(0.1.3)
LICENSE("MIT OR Apache-2.0")
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../entities.rs
    ../lib.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../local_names.txt
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/phf_codegen-0.11.3
    contrib/crates/string_cache_codegen-0.5.4
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
