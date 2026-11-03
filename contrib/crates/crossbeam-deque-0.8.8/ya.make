# Native Rust build with dependencies shared through contrib/crates.

CRATE(crossbeam_deque)

CRATE_VERSION(0.8.8)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/deque.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/crossbeam-epoch-0.9.21
    contrib/crates/crossbeam-utils-0.8.23
)

CRATE_BUILD_SCRIPT(contrib/crates/crossbeam-deque-0.8.8/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/crossbeam-deque/crossbeam-deque-0.8.8.crate)

END()

RECURSE(build_script)
