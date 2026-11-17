# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE_PROC_MACRO(openssl_macros)

CRATE_VERSION(0.1.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/openssl-macros/openssl-macros-0.1.1.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
