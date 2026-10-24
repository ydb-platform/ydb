# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(equivalent)

CRATE_VERSION(1.0.2)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.0.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/equivalent/equivalent-1.0.2.crate)

END()
