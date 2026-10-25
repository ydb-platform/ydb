# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(target_lexicon)

CRATE_VERSION(0.13.5)
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
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

CRATE_BUILD_SCRIPT(contrib/crates/target-lexicon-0.13.5/build_script)

LICENSE("Apache-2.0 WITH LLVM-exception")
VERSION(0.13.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/target-lexicon/target-lexicon-0.13.5.crate)

CRATE_FEATURES(
    std
)

CRATE_SOURCES(
    src/data_model.rs
    src/targets.rs
    src/triple.rs
)

CRATE_SOURCES(
    src/host.rs
    src/lib.rs
    src/parse_error.rs
)

END()

RECURSE(build_script)
