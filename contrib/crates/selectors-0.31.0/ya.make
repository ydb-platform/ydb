# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(selectors)

CRATE_VERSION(0.31.0)
CRATE_EDITION(2021)
CRATE_ROOT(lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.31.0)
LICENSE("MPL-2.0")

CRATE_SOURCES(
    attr.rs
    bloom.rs
    build.rs
    builder.rs
    context.rs
    kleene_value.rs
    matching.rs
    nth_index_cache.rs
    parser.rs
    relative_selector/cache.rs
    relative_selector/filter.rs
    relative_selector/mod.rs
    sink.rs
    tree.rs
    visitor.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGES.md
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


CRATE_BUILD_SCRIPT(contrib/crates/selectors-0.31.0/build_script)
PEERDIR(
    contrib/crates/bitflags-2.13.1
    contrib/crates/cssparser-0.35.0
    contrib/crates/derive_more-2.1.1
    contrib/crates/fxhash-0.2.1
    contrib/crates/log-0.4.33
    contrib/crates/new_debug_unreachable-1.0.6
    contrib/crates/phf-0.11.3
    contrib/crates/precomputed-hash-0.1.1
    contrib/crates/servo_arc-0.4.3
    contrib/crates/smallvec-1.15.2
)
CRATE_PROC_MACROS(
    contrib/crates/cssparser-macros-0.6.1
    contrib/crates/derive_more-impl-2.1.1
    contrib/crates/phf_macros-0.11.3
)

END()

RECURSE(build_script)
