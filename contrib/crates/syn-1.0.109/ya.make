# Rust sources pinned by cryptography 41.0.6 Cargo.lock.

CRATE(syn)

CRATE_VERSION(1.0.109)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    build.rs
    src/attr.rs
    src/await.rs
    src/bigint.rs
    src/buffer.rs
    src/custom_keyword.rs
    src/custom_punctuation.rs
    src/data.rs
    src/derive.rs
    src/discouraged.rs
    src/drops.rs
    src/error.rs
    src/export.rs
    src/expr.rs
    src/ext.rs
    src/file.rs
    src/gen/clone.rs
    src/gen/debug.rs
    src/gen/eq.rs
    src/gen/fold.rs
    src/gen/hash.rs
    src/gen/visit.rs
    src/gen/visit_mut.rs
    src/gen_helper.rs
    src/generics.rs
    src/group.rs
    src/ident.rs
    src/item.rs
    src/lifetime.rs
    src/lit.rs
    src/lookahead.rs
    src/mac.rs
    src/macros.rs
    src/op.rs
    src/parse.rs
    src/parse_macro_input.rs
    src/parse_quote.rs
    src/pat.rs
    src/path.rs
    src/print.rs
    src/punctuated.rs
    src/reserved.rs
    src/sealed.rs
    src/span.rs
    src/spanned.rs
    src/stmt.rs
    src/thread.rs
    src/token.rs
    src/tt.rs
    src/ty.rs
    src/verbatim.rs
    src/whitespace.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FEATURES(
    clone-impls
    default
    derive
    extra-traits
    full
    parsing
    printing
    proc-macro
    quote
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/unicode-ident-1.0.27
)

CRATE_BUILD_SCRIPT(contrib/crates/syn-1.0.109/build_script)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.109)
ORIGINAL_SOURCE(https://static.crates.io/crates/syn/syn-1.0.109.crate)

CRATE_FEATURES(
    fold
    visit
    visit-mut
)

CRATE_SOURCES(
    src/await.rs
    src/discouraged.rs
    src/export.rs
)

CRATE_SOURCES(
    src/attr.rs
    src/bigint.rs
    src/buffer.rs
    src/custom_keyword.rs
    src/custom_punctuation.rs
    src/data.rs
    src/derive.rs
    src/drops.rs
    src/error.rs
    src/expr.rs
    src/ext.rs
    src/file.rs
    src/gen/clone.rs
    src/gen/debug.rs
    src/gen/eq.rs
    src/gen/fold.rs
    src/gen/hash.rs
    src/gen/visit.rs
    src/gen/visit_mut.rs
    src/gen_helper.rs
    src/generics.rs
    src/group.rs
    src/ident.rs
    src/item.rs
    src/lib.rs
    src/lifetime.rs
    src/lit.rs
    src/lookahead.rs
    src/mac.rs
    src/macros.rs
    src/op.rs
    src/parse.rs
    src/parse_macro_input.rs
    src/parse_quote.rs
    src/pat.rs
    src/path.rs
    src/print.rs
    src/punctuated.rs
    src/reserved.rs
    src/sealed.rs
    src/span.rs
    src/spanned.rs
    src/stmt.rs
    src/thread.rs
    src/token.rs
    src/tt.rs
    src/ty.rs
    src/verbatim.rs
    src/whitespace.rs
)

END()

RECURSE(build_script)
