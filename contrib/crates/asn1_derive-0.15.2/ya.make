# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE_PROC_MACRO(asn1_derive)

CRATE_VERSION(0.15.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("BSD-3-Clause")
VERSION(0.15.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/asn1_derive/asn1_derive-0.15.2.crate)

END()
