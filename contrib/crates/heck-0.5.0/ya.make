# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(heck)

CRATE_VERSION(0.5.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.5.0)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/kebab.rs
    src/lower_camel.rs
    src/shouty_kebab.rs
    src/shouty_snake.rs
    src/snake.rs
    src/title.rs
    src/train.rs
    src/upper_camel.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)




CRATE_SOURCES(
    src/kebab.rs
    src/lib.rs
    src/lower_camel.rs
    src/shouty_kebab.rs
    src/shouty_snake.rs
    src/snake.rs
    src/title.rs
    src/train.rs
    src/upper_camel.rs
)

END()
