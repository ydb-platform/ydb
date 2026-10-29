# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(prost_build)

CRATE_VERSION(0.14.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ast.rs
    src/code_generator.rs
    src/code_generator/c_escaping.rs
    src/code_generator/syntax.rs
    src/collections.rs
    src/config.rs
    src/context.rs
    src/extern_paths.rs
    src/fixtures/alphabet/_expected_include.rs
    src/fixtures/deprecated/_all_deprecated.rs
    src/fixtures/field_attributes/_expected_field_attributes.rs
    src/fixtures/field_attributes/_expected_field_attributes_formatted.rs
    src/fixtures/helloworld/_expected_helloworld.rs
    src/fixtures/imports_empty/_expected_include.rs
    src/fixtures/smoke_test/_expected_smoke_test.rs
    src/fixtures/smoke_test/_expected_smoke_test_formatted.rs
    src/fixtures/write_includes/_.includes.rs
    src/ident.rs
    src/message_graph.rs
    src/module.rs
    src/path.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
    src/fixtures/alphabet/a.proto
    src/fixtures/alphabet/b.proto
    src/fixtures/alphabet/c.proto
    src/fixtures/alphabet/d.proto
    src/fixtures/alphabet/e.proto
    src/fixtures/alphabet/f.proto
    src/fixtures/deprecated/all_deprecated.proto
    src/fixtures/field_attributes/field_attributes.proto
    src/fixtures/helloworld/goodbye.proto
    src/fixtures/helloworld/hello.proto
    src/fixtures/helloworld/types.proto
    src/fixtures/imports_empty/imports_empty.proto
    src/fixtures/smoke_test/smoke_test.proto
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    cleanup-markdown
    default
    format
)

PEERDIR(
    contrib/crates/heck-0.5.0
    contrib/crates/itertools-0.14.0
    contrib/crates/log-0.4.34
    contrib/crates/multimap-0.10.1
    contrib/crates/petgraph-0.8.3
    contrib/crates/prettyplease-0.2.37
    contrib/crates/prost-0.14.4
    contrib/crates/prost-types-0.14.4
    contrib/crates/pulldown-cmark-0.13.5
    contrib/crates/pulldown-cmark-to-cmark-22.0.3
    contrib/crates/regex-1.13.1
    contrib/crates/syn-2.0.119
    contrib/crates/tempfile-3.27.0
)

LICENSE("Apache-2.0")
VERSION(0.14.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/prost-build/prost-build-0.14.4.crate)

CRATE_SOURCES(
    src/ast.rs
    src/code_generator.rs
    src/code_generator/c_escaping.rs
    src/code_generator/syntax.rs
    src/collections.rs
    src/config.rs
    src/context.rs
    src/extern_paths.rs
    src/fixtures/alphabet/_expected_include.rs
    src/fixtures/deprecated/_all_deprecated.rs
    src/fixtures/field_attributes/_expected_field_attributes.rs
    src/fixtures/field_attributes/_expected_field_attributes_formatted.rs
    src/fixtures/helloworld/_expected_helloworld.rs
    src/fixtures/imports_empty/_expected_include.rs
    src/fixtures/smoke_test/_expected_smoke_test.rs
    src/fixtures/smoke_test/_expected_smoke_test_formatted.rs
    src/fixtures/write_includes/_.includes.rs
    src/ident.rs
    src/lib.rs
    src/message_graph.rs
    src/module.rs
    src/path.rs
)

CRATE_INPUTS(
    src/fixtures/alphabet/a.proto
    src/fixtures/alphabet/b.proto
    src/fixtures/alphabet/c.proto
    src/fixtures/alphabet/d.proto
    src/fixtures/alphabet/e.proto
    src/fixtures/alphabet/f.proto
    src/fixtures/deprecated/all_deprecated.proto
    src/fixtures/field_attributes/field_attributes.proto
    src/fixtures/helloworld/goodbye.proto
    src/fixtures/helloworld/hello.proto
    src/fixtures/helloworld/types.proto
    src/fixtures/imports_empty/imports_empty.proto
    src/fixtures/smoke_test/smoke_test.proto
)

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/prost-derive-0.14.4
    contrib/crates/serde_derive-1.0.229
)

END()
