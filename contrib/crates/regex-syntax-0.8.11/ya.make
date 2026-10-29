# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(regex_syntax)

CRATE_VERSION(0.8.11)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ast/mod.rs
    src/ast/parse.rs
    src/ast/print.rs
    src/ast/visitor.rs
    src/debug.rs
    src/either.rs
    src/error.rs
    src/hir/interval.rs
    src/hir/literal.rs
    src/hir/mod.rs
    src/hir/print.rs
    src/hir/translate.rs
    src/hir/visitor.rs
    src/parser.rs
    src/rank.rs
    src/unicode.rs
    src/unicode_tables/age.rs
    src/unicode_tables/case_folding_simple.rs
    src/unicode_tables/general_category.rs
    src/unicode_tables/grapheme_cluster_break.rs
    src/unicode_tables/mod.rs
    src/unicode_tables/perl_decimal.rs
    src/unicode_tables/perl_space.rs
    src/unicode_tables/perl_word.rs
    src/unicode_tables/property_bool.rs
    src/unicode_tables/property_names.rs
    src/unicode_tables/property_values.rs
    src/unicode_tables/script.rs
    src/unicode_tables/script_extension.rs
    src/unicode_tables/sentence_break.rs
    src/unicode_tables/word_break.rs
    src/utf8.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    src/unicode_tables/LICENSE-UNICODE
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
    unicode
    unicode-age
    unicode-bool
    unicode-case
    unicode-gencat
    unicode-perl
    unicode-script
    unicode-segment
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.11)
ORIGINAL_SOURCE(https://static.crates.io/crates/regex-syntax/regex-syntax-0.8.11.crate)

CRATE_SOURCES(
    src/ast/mod.rs
    src/ast/parse.rs
    src/ast/print.rs
    src/ast/visitor.rs
    src/debug.rs
    src/either.rs
    src/error.rs
    src/hir/interval.rs
    src/hir/literal.rs
    src/hir/mod.rs
    src/hir/print.rs
    src/hir/translate.rs
    src/hir/visitor.rs
    src/lib.rs
    src/parser.rs
    src/rank.rs
    src/unicode.rs
    src/unicode_tables/age.rs
    src/unicode_tables/case_folding_simple.rs
    src/unicode_tables/general_category.rs
    src/unicode_tables/grapheme_cluster_break.rs
    src/unicode_tables/mod.rs
    src/unicode_tables/perl_decimal.rs
    src/unicode_tables/perl_space.rs
    src/unicode_tables/perl_word.rs
    src/unicode_tables/property_bool.rs
    src/unicode_tables/property_names.rs
    src/unicode_tables/property_values.rs
    src/unicode_tables/script.rs
    src/unicode_tables/script_extension.rs
    src/unicode_tables/sentence_break.rs
    src/unicode_tables/word_break.rs
    src/utf8.rs
)

CRATE_INPUTS(
    src/unicode_tables/LICENSE-UNICODE
)

END()
