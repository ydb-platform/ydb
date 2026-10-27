# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(predicates)

CRATE_VERSION(3.1.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/boolean.rs
    src/boxed.rs
    src/color.rs
    src/constant.rs
    src/float/close.rs
    src/float/mod.rs
    src/function.rs
    src/iter.rs
    src/name.rs
    src/ord.rs
    src/path/existence.rs
    src/path/fc.rs
    src/path/fs.rs
    src/path/ft.rs
    src/path/mod.rs
    src/prelude.rs
    src/str/adapters.rs
    src/str/basics.rs
    src/str/difference.rs
    src/str/mod.rs
    src/str/normalize.rs
    src/str/regex.rs
    src/utils.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/anstyle-1.0.14
    contrib/crates/predicates-core-1.0.10
)

LICENSE("MIT OR Apache-2.0")
VERSION(3.1.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/predicates/predicates-3.1.4.crate)

CRATE_INPUTS(
    README.md
)

CRATE_SOURCES(
    src/boolean.rs
    src/boxed.rs
    src/color.rs
    src/constant.rs
    src/float/close.rs
    src/float/mod.rs
    src/function.rs
    src/iter.rs
    src/lib.rs
    src/name.rs
    src/ord.rs
    src/path/existence.rs
    src/path/fc.rs
    src/path/fs.rs
    src/path/ft.rs
    src/path/mod.rs
    src/prelude.rs
    src/str/adapters.rs
    src/str/basics.rs
    src/str/difference.rs
    src/str/mod.rs
    src/str/normalize.rs
    src/str/regex.rs
    src/utils.rs
)

END()
