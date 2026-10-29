# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(matchers)

CRATE_VERSION(0.2.0)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .clog.toml
    .gitignore
    CHANGELOG.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/regex-automata-0.4.18
)

LICENSE("MIT")
VERSION(0.2.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/matchers/matchers-0.2.0.crate)

CRATE_SOURCES(
    src/lib.rs
)

END()
