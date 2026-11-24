# Native Rust build with dependencies shared through contrib/crates.

CRATE(mime_guess)

CRATE_VERSION(2.0.5)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/impl_bin_search.rs
    src/impl_phf.rs
    src/mime_types.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    rev-mappings
)

PEERDIR(
    contrib/crates/mime-0.3.17
    contrib/crates/unicase-2.10.0
)

CRATE_BUILD_SCRIPT(contrib/crates/mime_guess-2.0.5/build_script)

LICENSE("MIT")
VERSION(2.0.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/mime_guess/mime_guess-2.0.5.crate)

END()

RECURSE(build_script)
