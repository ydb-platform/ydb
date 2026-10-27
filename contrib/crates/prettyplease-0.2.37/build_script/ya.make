# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(0.2.37)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/algorithm.rs
    ../src/attr.rs
    ../src/classify.rs
    ../src/convenience.rs
    ../src/data.rs
    ../src/expr.rs
    ../src/file.rs
    ../src/fixup.rs
    ../src/generics.rs
    ../src/item.rs
    ../src/iter.rs
    ../src/lib.rs
    ../src/lifetime.rs
    ../src/lit.rs
    ../src/mac.rs
    ../src/pat.rs
    ../src/path.rs
    ../src/precedence.rs
    ../src/ring.rs
    ../src/stmt.rs
    ../src/token.rs
    ../src/ty.rs
)

CRATE_INPUTS(
    ../.cargo-checksum.json
    ../.cargo_vcs_info.json
    ../.gitattributes
    ../.gitignore
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.37)
ORIGINAL_SOURCE(https://static.crates.io/crates/prettyplease/prettyplease-0.2.37.crate)

CRATE_SOURCES(
    ../src/algorithm.rs
    ../src/attr.rs
    ../src/classify.rs
    ../src/convenience.rs
    ../src/data.rs
    ../src/expr.rs
    ../src/file.rs
    ../src/fixup.rs
    ../src/generics.rs
    ../src/item.rs
    ../src/iter.rs
    ../src/lib.rs
    ../src/lifetime.rs
    ../src/lit.rs
    ../src/mac.rs
    ../src/pat.rs
    ../src/path.rs
    ../src/precedence.rs
    ../src/ring.rs
    ../src/stmt.rs
    ../src/token.rs
    ../src/ty.rs
)

END()
