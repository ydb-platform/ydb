# Native Rust build with dependencies shared through contrib/crates.

CRATE(core_foundation)

CRATE_VERSION(0.9.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/array.rs
    src/attributed_string.rs
    src/base.rs
    src/boolean.rs
    src/bundle.rs
    src/characterset.rs
    src/data.rs
    src/date.rs
    src/dictionary.rs
    src/error.rs
    src/filedescriptor.rs
    src/mach_port.rs
    src/number.rs
    src/propertylist.rs
    src/runloop.rs
    src/set.rs
    src/string.rs
    src/timezone.rs
    src/url.rs
    src/uuid.rs
)

CRATE_INPUTS(
    LICENSE-APACHE
    LICENSE-MIT
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    link
)

PEERDIR(
    contrib/crates/core-foundation-sys-0.8.7
    contrib/crates/libc-0.2.191
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.9.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/core-foundation/core-foundation-0.9.4.crate)

END()
