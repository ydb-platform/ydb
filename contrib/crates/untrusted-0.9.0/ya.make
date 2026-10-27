# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(untrusted)

CRATE_VERSION(0.9.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/input.rs
    src/no_panic.rs
    src/reader.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitattributes
    .gitignore
    LICENSE.txt
    README.md
    deny.toml
    mk/cargo.sh
    mk/install-build-tools.sh
    mk/llvm-snapshot.gpg.key
    mk/runner
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("ISC")
VERSION(0.9.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/untrusted/untrusted-0.9.0.crate)

CRATE_SOURCES(
    src/input.rs
    src/lib.rs
    src/no_panic.rs
    src/reader.rs
)

END()
