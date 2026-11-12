# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(tonic_build)

CRATE_VERSION(0.14.6)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/client.rs
    src/code_gen.rs
    src/manual.rs
    src/server.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    transport
)

PEERDIR(
    contrib/crates/prettyplease-0.2.37
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT")
VERSION(0.14.6)
ORIGINAL_SOURCE(https://static.crates.io/crates/tonic-build/tonic-build-0.14.6.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/client.rs
    src/code_gen.rs
    src/lib.rs
    src/manual.rs
    src/server.rs
)

END()
