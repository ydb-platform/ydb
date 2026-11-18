# Native Rust dependency shared through contrib/crates.

CRATE(lru)

CRATE_VERSION(0.18.5)

CRATE_EDITION(2015)

CRATE_ROOT(src/lib.rs)

CRATE_MANIFEST(Cargo.toml)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    hashbrown
)

PEERDIR(
    contrib/crates/hashbrown-0.17.1
)

LICENSE("MIT")

VERSION(0.18.5)

ORIGINAL_SOURCE(https://static.crates.io/crates/lru/lru-0.18.5.crate)

CRATE_INPUTS(
    .cargo_vcs_info.json
    .github/workflows/main.yml
    .gitignore
    CHANGELOG.md
    Cargo.lock
    Cargo.toml
    Cargo.toml.orig
    LICENSE
    README.md
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

END()
