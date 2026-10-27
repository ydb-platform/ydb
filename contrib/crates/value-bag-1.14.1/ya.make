# Native Rust build with dependencies shared through contrib/crates.

CRATE(value_bag)

CRATE_VERSION(1.14.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/error.rs
    src/fill.rs
    src/impls.rs
    src/internal/cast/mod.rs
    src/internal/cast/primitive.rs
    src/internal/error.rs
    src/internal/fmt.rs
    src/internal/mod.rs
    src/internal/owned.rs
    src/internal/seq.rs
    src/internal/serde/mod.rs
    src/internal/serde/v1.rs
    src/internal/sval/mod.rs
    src/internal/sval/v2.rs
    src/owned.rs
    src/test.rs
    src/visit.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    inline-i128
)

LICENSE("Apache-2.0 OR MIT")
VERSION(1.14.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/value-bag/value-bag-1.14.1.crate)

END()
