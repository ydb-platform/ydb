# Native Rust build with dependencies shared through contrib/crates.

CRATE(http)

CRATE_VERSION(1.5.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/byte_str.rs
    src/convert.rs
    src/error.rs
    src/extensions.rs
    src/header/map.rs
    src/header/mod.rs
    src/header/name.rs
    src/header/value.rs
    src/method.rs
    src/request.rs
    src/response.rs
    src/status.rs
    src/uri/authority.rs
    src/uri/builder.rs
    src/uri/mod.rs
    src/uri/path.rs
    src/uri/port.rs
    src/uri/scheme.rs
    src/uri/tests.rs
    src/version.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/itoa-1.0.18
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.5.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/http/http-1.5.0.crate)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
