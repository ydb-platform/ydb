# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(target_lexicon)

CRATE_VERSION(0.12.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    examples/host.rs
    examples/misc.rs
    src/data_model.rs
    src/host.rs
    src/parse_error.rs
    src/targets.rs
    src/triple.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    .rustfmt.toml
    Cargo.lock
    LICENSE
    README.md
    scripts/rust-targets.sh
)

CRATE_FEATURES(
    default
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/target-lexicon-0.12.7/build_script)

LICENSE("Apache-2.0 WITH LLVM-exception")
VERSION(0.12.7)
ORIGINAL_SOURCE(https://static.crates.io/crates/target-lexicon/target-lexicon-0.12.7.crate)

END()

RECURSE(build_script)
