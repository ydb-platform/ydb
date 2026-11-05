# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zmij)

CRATE_VERSION(1.0.23)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/stdarch_x86.rs
    src/tests.rs
    src/traits.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/zmij-1.0.23/build_script)

LICENSE("MIT")
VERSION(1.0.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/zmij/zmij-1.0.23.crate)

CRATE_SOURCES(
    src/lib.rs
    src/stdarch_x86.rs
    src/tests.rs
    src/traits.rs
)

END()

RECURSE(build_script)
