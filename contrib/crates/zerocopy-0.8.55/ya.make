# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(zerocopy)

CRATE_VERSION(0.8.55)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/byte_slice.rs
    src/byteorder.rs
    src/deprecated.rs
    src/doctests.rs
    src/error.rs
    src/impls.rs
    src/layout.rs
    src/macros.rs
    src/pointer/inner.rs
    src/pointer/invariant.rs
    src/pointer/mod.rs
    src/pointer/ptr.rs
    src/pointer/transmute.rs
    src/ref.rs
    src/split_at.rs
    src/util/macro_util.rs
    src/util/macros.rs
    src/util/mod.rs
    src/wrappers.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    AGENTS.md
    CHANGELOG.md
    Cargo.lock
    Cargo.toml.std
    LICENSE-APACHE
    LICENSE-BSD
    LICENSE-MIT
    POLICIES.md
    README.md
    agent_docs/development.md
    agent_docs/reviewing.md
    agent_docs/style.md
    agent_docs/ui_tests.md
    agent_docs/unsafe_code.md
    agent_docs/validation.md
    cargo.sh
    clippy.toml
    rustdoc/style.css
    testdata/include_value/data
    win-cargo.bat
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    simd
)

CRATE_BUILD_SCRIPT(contrib/crates/zerocopy-0.8.55/build_script)

LICENSE("BSD-2-Clause OR Apache-2.0 OR MIT")
VERSION(0.8.55)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerocopy/zerocopy-0.8.55.crate)

END()

RECURSE(build_script)
