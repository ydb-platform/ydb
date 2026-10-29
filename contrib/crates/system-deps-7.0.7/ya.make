# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(system_deps)

CRATE_VERSION(7.0.7)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(7.0.7)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/metadata.rs
    src/test.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    src/tests/testanotherlib.pc
    src/tests/testdata.pc
    src/tests/testlib-2.0.pc
    src/tests/testlib-3.0.pc
    src/tests/testlib.pc
    src/tests/testlibwithrpath.pc
    src/tests/teststaticlib.pc
    src/tests/toml-missing-file/no-cargo-toml-here
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/cfg-expr-0.20.6
    contrib/crates/heck-0.5.0
    contrib/crates/pkg-config-0.3.34
    contrib/crates/toml-1.1.3+spec-1.1.0
    contrib/crates/version-compare-0.2.1
)

CRATE_PROC_MACROS(
    contrib/crates/serde_derive-1.0.229
)

END()
