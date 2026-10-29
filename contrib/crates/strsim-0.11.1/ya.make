# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(strsim)

CRATE_VERSION(0.11.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .editorconfig
    .gitattributes
    .gitignore
    CHANGELOG.md
    LICENSE
    README.md
    SECURITY.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.11.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/strsim/strsim-0.11.1.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
