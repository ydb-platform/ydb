# Native Rust build with dependencies shared through contrib/crates.

CRATE(target_lexicon)

CRATE_VERSION(0.12.16)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/data_model.rs
    src/host.rs
    src/parse_error.rs
    src/targets.rs
    src/triple.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
    scripts/rust-targets.sh
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

CRATE_BUILD_SCRIPT(contrib/crates/target-lexicon-0.12.16/build_script)

LICENSE("Apache-2.0 WITH LLVM-exception")
VERSION(0.12.16)
ORIGINAL_SOURCE(https://static.crates.io/crates/target-lexicon/target-lexicon-0.12.16.crate)

END()

RECURSE(build_script)
