# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.13.5)
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
    ../Cargo.lock
    ../LICENSE
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.13.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/pulldown-cmark/pulldown-cmark-0.13.5.crate)

END()
