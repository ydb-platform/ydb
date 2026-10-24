# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(nu_ansi_term)

CRATE_VERSION(0.50.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ansi.rs
    src/debug.rs
    src/difference.rs
    src/display.rs
    src/gradient.rs
    src/rgb.rs
    src/style.rs
    src/util.rs
    src/windows.rs
    src/write.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/windows-sys-0.61.2
    )

    CRATE_DEPENDENCY_ALIAS(windows contrib/crates/windows-sys-0.61.2)
ENDIF()

LICENSE("MIT")
VERSION(0.50.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/nu-ansi-term/nu-ansi-term-0.50.3.crate)

END()
