# Native Rust build with dependencies shared through contrib/crates.

CRATE(want)

CRATE_VERSION(0.3.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/try-lock-0.2.5
)

LICENSE("MIT")
VERSION(0.3.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/want/want-0.3.2.crate)


END()
