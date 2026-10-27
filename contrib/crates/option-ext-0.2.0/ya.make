# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(option_ext)

CRATE_VERSION(0.2.0)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/impl.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MPL-2.0")
VERSION(0.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/option-ext/option-ext-0.2.0.crate)

CRATE_SOURCES(
    src/impl.rs
    src/lib.rs
)

END()
