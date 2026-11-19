# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(windows_result)

CRATE_VERSION(0.4.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/bindings.rs
    src/bool.rs
    src/bstr.rs
    src/com.rs
    src/error.rs
    src/hresult.rs
    src/strings.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    license-apache-2.0
    license-mit
    readme.md
    windows-result.natvis
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    std
)

PEERDIR(
    contrib/crates/windows-link-0.2.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/windows-result/windows-result-0.4.1.crate)

CRATE_INPUTS(
    readme.md
)

CRATE_SOURCES(
    src/bindings.rs
    src/bool.rs
    src/bstr.rs
    src/com.rs
    src/error.rs
    src/hresult.rs
    src/lib.rs
    src/strings.rs
)

END()
