# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(async_trait)

CRATE_VERSION(0.1.91)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/args.rs
    src/bound.rs
    src/expand.rs
    src/lifetime.rs
    src/parse.rs
    src/receiver.rs
    src/verbatim.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.3
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.1.91)
ORIGINAL_SOURCE(https://static.crates.io/crates/async-trait/async-trait-0.1.91.crate)

END()
