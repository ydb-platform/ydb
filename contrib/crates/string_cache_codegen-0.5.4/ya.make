# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(string_cache_codegen)

CRATE_VERSION(0.5.4)
CRATE_EDITION(2018)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.5.4)
LICENSE("MIT OR Apache-2.0")

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/phf_generator-0.11.3
    contrib/crates/phf_shared-0.13.1
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
