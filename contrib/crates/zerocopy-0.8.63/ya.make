# Native Rust build with dependencies shared through contrib/crates.

CRATE(zerocopy)

CRATE_VERSION(0.8.63)
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
    AGENTS.md
    CHANGELOG.md
    Cargo.lock
    Cargo.toml.std
    LICENSE-APACHE
    LICENSE-BSD
    LICENSE-MIT
    POLICIES.md
    README.md
    cargo.sh
    ci/check_all_toolchains_tested.sh
    ci/check_fmt.sh
    ci/check_msrv_is_minimal.sh
    ci/check_packaging.sh
    ci/check_readme.sh
    ci/check_stale_stderr.sh
    ci/check_versions.sh
    ci/release_crate_version.sh
    clippy.toml
    rustdoc/style.css
    testdata/include_value/data
    win-cargo.bat
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    simd
)

CRATE_BUILD_SCRIPT(contrib/crates/zerocopy-0.8.63/build_script)

LICENSE("BSD-2-Clause OR Apache-2.0 OR MIT")
VERSION(0.8.63)
ORIGINAL_SOURCE(https://static.crates.io/crates/zerocopy/zerocopy-0.8.63.crate)

CRATE_FEATURES(
    derive
    zerocopy-derive
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()

RECURSE(build_script)
