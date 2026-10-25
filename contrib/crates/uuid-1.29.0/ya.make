# Native Rust build with dependencies shared through contrib/crates.

CRATE(uuid)

CRATE_VERSION(1.29.0)
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
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    rng
    std
    v4
)

PEERDIR(
    contrib/crates/getrandom-0.4.3
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.29.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/uuid/uuid-1.29.0.crate)

CRATE_FEATURES(
    atomic
    fast-rng
    js
    md5
    serde
    sha1
    v1
    v3
    v5
    v6
    v7
    v8
)

PEERDIR(
    contrib/crates/atomic-0.6.1
    contrib/crates/md-5-0.10.6
    contrib/crates/rand-0.10.3
    contrib/crates/serde_core-1.0.229
    contrib/crates/sha1_smol-1.0.1
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()
