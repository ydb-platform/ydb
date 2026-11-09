# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(language_tags)

CRATE_VERSION(0.3.2)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.3.2)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/iana_registry.rs
    src/serde.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE
    README.md
    scripts/update_from_iana_subtag_registery.py
)

CRATE_FLAGS(--cap-lints=allow)




END()
