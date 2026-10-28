# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(ouroboros)

CRATE_VERSION(0.15.6)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE_APACHE
    LICENSE_MIT
    README.md
)

CRATE_FEATURES(
    default
    std
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/aliasable-0.1.3
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.15.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/ouroboros/ouroboros-0.15.6.crate)

CRATE_PROC_MACROS(
    contrib/crates/ouroboros_macro-0.15.6
    contrib/crates/proc-macro-error-attr-1.0.4
)

END()
