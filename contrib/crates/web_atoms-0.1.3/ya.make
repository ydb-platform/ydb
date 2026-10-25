# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(web_atoms)

CRATE_VERSION(0.1.3)
CRATE_EDITION(2021)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.1.3)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    build.rs
    entities.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    local_names.txt
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/web_atoms-0.1.3/build_script)
PEERDIR(
    contrib/crates/phf-0.11.3
    contrib/crates/string_cache-0.9.0
)

CRATE_PROC_MACROS(
    contrib/crates/phf_macros-0.11.3
    contrib/crates/serde_derive-1.0.229
    contrib/crates/zerocopy-derive-0.8.63
)

END()

RECURSE(build_script)
