# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE_PROC_MACRO(rustversion)

CRATE_VERSION(1.0.23)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build/build.rs
    build/rustc.rs
    src/attr.rs
    src/bound.rs
    src/constfn.rs
    src/date.rs
    src/error.rs
    src/expand.rs
    src/expr.rs
    src/iter.rs
    src/release.rs
    src/time.rs
    src/token.rs
    src/version.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_BUILD_SCRIPT(contrib/crates/rustversion-1.0.23/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.23)
ORIGINAL_SOURCE(https://static.crates.io/crates/rustversion/rustversion-1.0.23.crate)

CRATE_SOURCES(
    build/build.rs
    build/rustc.rs
    src/attr.rs
    src/bound.rs
    src/constfn.rs
    src/date.rs
    src/error.rs
    src/expand.rs
    src/expr.rs
    src/iter.rs
    src/lib.rs
    src/release.rs
    src/time.rs
    src/token.rs
    src/version.rs
)

END()

RECURSE(build_script)
