# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(lru)

CRATE_VERSION(0.18.1)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    hashbrown
)

PEERDIR(
    contrib/crates/hashbrown-0.17.1
)

LICENSE("MIT")
VERSION(0.18.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/lru/lru-0.18.1.crate)

END()
