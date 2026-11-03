# Native Rust build with dependencies shared through contrib/crates.

CRATE(cpufeatures)

CRATE_VERSION(0.3.1)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/aarch64.rs
    src/loongarch64.rs
    src/miri.rs
    src/x86.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.3.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/cpufeatures/cpufeatures-0.3.1.crate)

END()
