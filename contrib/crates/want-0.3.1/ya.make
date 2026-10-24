# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(want)

CRATE_VERSION(0.3.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/try-lock-0.2.5
)

LICENSE("MIT")
VERSION(0.3.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/want/want-0.3.1.crate)

END()
