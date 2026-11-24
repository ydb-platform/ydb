# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(asn1)

CRATE_VERSION(0.15.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    examples/no_std.rs
    src/base128.rs
    src/bit_string.rs
    src/object_identifier.rs
    src/parser.rs
    src/tag.rs
    src/types.rs
    src/writer.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .clusterfuzzlite/Dockerfile
    .clusterfuzzlite/build.sh
    .clusterfuzzlite/project.yaml
    .gitignore
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("BSD-3-Clause")
VERSION(0.15.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/asn1/asn1-0.15.2.crate)

CRATE_PROC_MACROS(
    contrib/crates/asn1_derive-0.15.2
)

END()
