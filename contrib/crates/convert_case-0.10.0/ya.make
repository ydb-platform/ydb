# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(convert_case)

CRATE_VERSION(0.10.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/boundary.rs
    src/case.rs
    src/converter.rs
    src/pattern.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .envrc
    .gitignore
    LICENSE
    README.md
    flake.lock
    flake.nix
    justfile
    rust-toolchain.toml
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/unicode-segmentation-1.13.3
)

LICENSE("MIT")
VERSION(0.10.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/convert_case/convert_case-0.10.0.crate)

CRATE_SOURCES(
    src/boundary.rs
    src/case.rs
    src/converter.rs
    src/lib.rs
    src/pattern.rs
)

END()
