# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(bytemuck_derive)

CRATE_VERSION(1.12.1)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/traits.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-ZLIB
    README.md
    changelog.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-3.0.9
)

LICENSE("Zlib OR Apache-2.0 OR MIT")
VERSION(1.12.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/bytemuck_derive/bytemuck_derive-1.12.1.crate)

END()
