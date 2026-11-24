# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(serde)

CRATE_VERSION(1.0.229)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/core/crate_root.rs
    src/core/de/ignored_any.rs
    src/core/de/impls.rs
    src/core/de/mod.rs
    src/core/de/value.rs
    src/core/format.rs
    src/core/lib.rs
    src/core/macros.rs
    src/core/private/content.rs
    src/core/private/doc.rs
    src/core/private/mod.rs
    src/core/private/seed.rs
    src/core/private/size_hint.rs
    src/core/private/string.rs
    src/core/ser/fmt.rs
    src/core/ser/impls.rs
    src/core/ser/impossible.rs
    src/core/ser/mod.rs
    src/core/std_error.rs
    src/integer128.rs
    src/private/de.rs
    src/private/mod.rs
    src/private/ser.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    crates-io.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    derive
    serde_derive
    std
)

PEERDIR(
    contrib/crates/serde_core-1.0.229
)

CRATE_BUILD_SCRIPT(contrib/crates/serde-1.0.229/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.229)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde/serde-1.0.229.crate)

CRATE_FEATURES(
    rc
)

CRATE_SOURCES(
    src/core/crate_root.rs
    src/core/macros.rs
)

CRATE_SOURCES(
    src/core/de/ignored_any.rs
    src/core/de/impls.rs
    src/core/de/mod.rs
    src/core/de/value.rs
    src/core/format.rs
    src/core/lib.rs
    src/core/private/content.rs
    src/core/private/doc.rs
    src/core/private/mod.rs
    src/core/private/seed.rs
    src/core/private/size_hint.rs
    src/core/private/string.rs
    src/core/ser/fmt.rs
    src/core/ser/impls.rs
    src/core/ser/impossible.rs
    src/core/ser/mod.rs
    src/core/std_error.rs
    src/integer128.rs
    src/lib.rs
    src/private/de.rs
    src/private/mod.rs
    src/private/ser.rs
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()

RECURSE(build_script)
