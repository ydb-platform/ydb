# Native Rust build with dependencies shared through contrib/crates.

CRATE(aho_corasick)

CRATE_VERSION(1.1.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/ahocorasick.rs
    src/automaton.rs
    src/dfa.rs
    src/macros.rs
    src/nfa/contiguous.rs
    src/nfa/mod.rs
    src/nfa/noncontiguous.rs
    src/packed/api.rs
    src/packed/ext.rs
    src/packed/mod.rs
    src/packed/pattern.rs
    src/packed/rabinkarp.rs
    src/packed/teddy/builder.rs
    src/packed/teddy/generic.rs
    src/packed/teddy/mod.rs
    src/packed/tests.rs
    src/packed/vector.rs
    src/tests.rs
    src/transducer.rs
    src/util/alphabet.rs
    src/util/buffer.rs
    src/util/byte_frequencies.rs
    src/util/debug.rs
    src/util/error.rs
    src/util/int.rs
    src/util/mod.rs
    src/util/prefilter.rs
    src/util/primitives.rs
    src/util/remapper.rs
    src/util/search.rs
    src/util/special.rs
)

CRATE_INPUTS(
    COPYING
    Cargo.lock
    DESIGN.md
    LICENSE-MIT
    README.md
    UNLICENSE
    rustfmt.toml
    src/packed/teddy/README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    perf-literal
    std
)

PEERDIR(
    contrib/crates/memchr-2.8.3
)

LICENSE("Unlicense OR MIT")
VERSION(1.1.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/aho-corasick/aho-corasick-1.1.5.crate)

CRATE_FEATURES(
    default
)

END()
