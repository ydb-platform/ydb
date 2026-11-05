# Native Rust build with dependencies shared through contrib/crates.

CRATE(http_range_header)

CRATE_VERSION(0.4.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_INPUTS(
    Changelog.md
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT")
VERSION(0.4.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/http-range-header/http-range-header-0.4.2.crate)

END()
