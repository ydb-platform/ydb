# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(typed_path)

CRATE_VERSION(0.12.3)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/common.rs
    src/common/errors.rs
    src/common/non_utf8.rs
    src/common/non_utf8/components.rs
    src/common/non_utf8/components/component.rs
    src/common/non_utf8/iter.rs
    src/common/non_utf8/parser.rs
    src/common/non_utf8/path.rs
    src/common/non_utf8/path/display.rs
    src/common/non_utf8/pathbuf.rs
    src/common/utf8.rs
    src/common/utf8/components.rs
    src/common/utf8/components/component.rs
    src/common/utf8/iter.rs
    src/common/utf8/path.rs
    src/common/utf8/pathbuf.rs
    src/native.rs
    src/platform.rs
    src/typed.rs
    src/typed/non_utf8.rs
    src/typed/non_utf8/components.rs
    src/typed/non_utf8/components/component.rs
    src/typed/non_utf8/iter.rs
    src/typed/non_utf8/path.rs
    src/typed/non_utf8/pathbuf.rs
    src/typed/utf8.rs
    src/typed/utf8/components.rs
    src/typed/utf8/components/component.rs
    src/typed/utf8/iter.rs
    src/typed/utf8/path.rs
    src/typed/utf8/pathbuf.rs
    src/unix.rs
    src/unix/constants.rs
    src/unix/non_utf8.rs
    src/unix/non_utf8/components.rs
    src/unix/non_utf8/components/component.rs
    src/unix/non_utf8/components/parser.rs
    src/unix/utf8.rs
    src/unix/utf8/components.rs
    src/unix/utf8/components/component.rs
    src/utils.rs
    src/windows.rs
    src/windows/constants.rs
    src/windows/non_utf8.rs
    src/windows/non_utf8/components.rs
    src/windows/non_utf8/components/component.rs
    src/windows/non_utf8/components/component/prefix.rs
    src/windows/non_utf8/components/parser.rs
    src/windows/utf8.rs
    src/windows/utf8/components.rs
    src/windows/utf8/components/component.rs
    src/windows/utf8/components/component/prefix.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    rustfmt.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.12.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/typed-path/typed-path-0.12.3.crate)

END()
