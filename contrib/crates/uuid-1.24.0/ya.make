# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(uuid)

CRATE_VERSION(1.24.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/builder.rs
    src/error.rs
    src/external.rs
    src/external/arbitrary_support.rs
    src/external/borsh_support.rs
    src/external/serde_support.rs
    src/external/slog_support.rs
    src/fmt.rs
    src/macros.rs
    src/md5.rs
    src/non_nil.rs
    src/parser.rs
    src/rng.rs
    src/sha1.rs
    src/timestamp.rs
    src/v1.rs
    src/v3.rs
    src/v4.rs
    src/v5.rs
    src/v6.rs
    src/v7.rs
    src/v8.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    rng
    v4
)

PEERDIR(
    contrib/crates/getrandom-0.4.3
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.24.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/uuid/uuid-1.24.0.crate)

END()
