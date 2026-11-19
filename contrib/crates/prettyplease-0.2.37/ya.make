# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(prettyplease)

CRATE_VERSION(0.2.37)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/algorithm.rs
    src/attr.rs
    src/classify.rs
    src/convenience.rs
    src/data.rs
    src/expr.rs
    src/file.rs
    src/fixup.rs
    src/generics.rs
    src/item.rs
    src/iter.rs
    src/lifetime.rs
    src/lit.rs
    src/mac.rs
    src/pat.rs
    src/path.rs
    src/precedence.rs
    src/ring.rs
    src/stmt.rs
    src/token.rs
    src/ty.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    .gitattributes
    .gitignore
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/syn-2.0.119
)

CRATE_BUILD_SCRIPT(contrib/crates/prettyplease-0.2.37/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(0.2.37)
ORIGINAL_SOURCE(https://static.crates.io/crates/prettyplease/prettyplease-0.2.37.crate)

CRATE_SOURCES(
    src/algorithm.rs
    src/attr.rs
    src/classify.rs
    src/convenience.rs
    src/data.rs
    src/expr.rs
    src/file.rs
    src/fixup.rs
    src/generics.rs
    src/item.rs
    src/iter.rs
    src/lib.rs
    src/lifetime.rs
    src/lit.rs
    src/mac.rs
    src/pat.rs
    src/path.rs
    src/precedence.rs
    src/ring.rs
    src/stmt.rs
    src/token.rs
    src/ty.rs
)

END()

RECURSE(build_script)
