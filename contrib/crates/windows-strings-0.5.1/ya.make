# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(windows_strings)

CRATE_VERSION(0.5.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bindings.rs
    src/bstr.rs
    src/decode.rs
    src/hstring.rs
    src/hstring_builder.rs
    src/hstring_header.rs
    src/literals.rs
    src/pcstr.rs
    src/pcwstr.rs
    src/pstr.rs
    src/pwstr.rs
    src/ref_count.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
    windows-strings.natvis
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/windows-link-0.2.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.5.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-strings/windows-strings-0.5.1.crate)

CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/bindings.rs
    src/bstr.rs
    src/decode.rs
    src/hstring.rs
    src/hstring_builder.rs
    src/hstring_header.rs
    src/lib.rs
    src/literals.rs
    src/pcstr.rs
    src/pcwstr.rs
    src/pstr.rs
    src/pwstr.rs
    src/ref_count.rs
)

END()
