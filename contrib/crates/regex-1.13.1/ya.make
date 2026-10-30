# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(regex)

CRATE_VERSION(1.13.1)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/builders.rs
    src/bytes.rs
    src/error.rs
    src/find_byte.rs
    src/pattern.rs
    src/regex/bytes.rs
    src/regex/mod.rs
    src/regex/string.rs
    src/regexset/bytes.rs
    src/regexset/mod.rs
    src/regexset/string.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    UNICODE.md
    bench/README.md
    testdata/anchored.toml
    testdata/bytes.toml
    testdata/crazy.toml
    testdata/crlf.toml
    testdata/earliest.toml
    testdata/empty.toml
    testdata/expensive.toml
    testdata/flags.toml
    testdata/iter.toml
    testdata/leftmost-all.toml
    testdata/line-terminator.toml
    testdata/misc.toml
    testdata/multiline.toml
    testdata/no-unicode.toml
    testdata/overlapping.toml
    testdata/regex-lite.toml
    testdata/regression.toml
    testdata/set.toml
    testdata/substring.toml
    testdata/unicode.toml
    testdata/utf8.toml
    testdata/word-boundary-special.toml
    testdata/word-boundary.toml
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    perf
    perf-backtrack
    perf-cache
    perf-dfa
    perf-inline
    perf-literal
    perf-onepass
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

PEERDIR(
    contrib/crates/aho-corasick-1.1.5
    contrib/crates/memchr-2.8.3
    contrib/crates/regex-automata-0.4.18
    contrib/crates/regex-syntax-0.8.11
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.13.1)
ORIGINAL_SOURCE(https://static.crates.io/crates/regex/regex-1.13.1.crate)

CRATE_SOURCES(
    src/builders.rs
    src/bytes.rs
    src/error.rs
    src/find_byte.rs
    src/lib.rs
    src/pattern.rs
    src/regex/bytes.rs
    src/regex/mod.rs
    src/regex/string.rs
    src/regexset/bytes.rs
    src/regexset/mod.rs
    src/regexset/string.rs
)

END()
