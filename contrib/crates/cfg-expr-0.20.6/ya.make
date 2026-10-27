# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(cfg_expr)

CRATE_VERSION(0.20.6)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.20.6)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    examples/eval.rs
    src/error.rs
    src/expr/lexer.rs
    src/expr/parser.rs
    src/expr.rs
    src/targets/builtins.rs
    src/targets.rs
)

CRATE_INPUTS(
    .cargo/config.toml
    .cargo-checksum.json
    .cargo_vcs_info.json
    .mergify.yml
    CHANGELOG.md
    CODE_OF_CONDUCT.md
    CONTRIBUTING.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    clippy.toml
    deny.toml
    release.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    target-lexicon
    targets
)
PEERDIR(
    contrib/crates/smallvec-1.16.4
    contrib/crates/target-lexicon-0.13.5
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
