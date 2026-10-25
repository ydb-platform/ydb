# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(pulldown_cmark)

CRATE_VERSION(0.13.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/entities.rs
    src/firstpass.rs
    src/html.rs
    src/linklabel.rs
    src/main.rs
    src/parse.rs
    src/puncttable.rs
    src/scanners.rs
    src/strings.rs
    src/tree.rs
    src/utils.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bitflags-2.13.1
    contrib/crates/memchr-2.8.3
    contrib/crates/unicase-2.9.0
)

CRATE_BUILD_SCRIPT(contrib/crates/pulldown-cmark-0.13.4/build_script)

LICENSE("MIT")
VERSION(0.13.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/pulldown-cmark/pulldown-cmark-0.13.4.crate)

END()

RECURSE(build_script)
