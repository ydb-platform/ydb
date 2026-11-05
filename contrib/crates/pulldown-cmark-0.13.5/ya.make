# Native Rust build with dependencies shared through contrib/crates.

CRATE(pulldown_cmark)

CRATE_VERSION(0.13.5)
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
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/bitflags-2.13.2
    contrib/crates/memchr-2.8.3
    contrib/crates/unicase-2.10.0
)

CRATE_BUILD_SCRIPT(contrib/crates/pulldown-cmark-0.13.5/build_script)

LICENSE("MIT")
VERSION(0.13.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/pulldown-cmark/pulldown-cmark-0.13.5.crate)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
)

END()

RECURSE(build_script)
