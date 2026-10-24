# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.13.4)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/entities.rs
    ../src/firstpass.rs
    ../src/html.rs
    ../src/lib.rs
    ../src/linklabel.rs
    ../src/main.rs
    ../src/parse.rs
    ../src/puncttable.rs
    ../src/scanners.rs
    ../src/strings.rs
    ../src/tree.rs
    ../src/utils.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../Cargo.lock
    ../LICENSE
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.13.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/pulldown-cmark/pulldown-cmark-0.13.4.crate)

END()
