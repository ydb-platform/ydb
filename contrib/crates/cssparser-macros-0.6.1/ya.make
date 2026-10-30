# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE_PROC_MACRO(cssparser_macros)

CRATE_VERSION(0.6.1)
CRATE_EDITION(2018)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.6.1)
LICENSE("MPL-2.0")


CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

END()
