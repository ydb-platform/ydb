# Native Rust build with dependencies shared through contrib/crates.

RUST_BUILD_SCRIPT(build_script_build)

CRATE_VERSION(1.0.154)
CRATE_EDITION(2021)
CRATE_ROOT(../build.rs)
CRATE_MANIFEST(../Cargo.toml)
CRATE_MANIFEST_DIR(..)

CRATE_SOURCES(
    ../src/de.rs
    ../src/error.rs
    ../src/io/core.rs
    ../src/io/mod.rs
    ../src/iter.rs
    ../src/lexical/algorithm.rs
    ../src/lexical/bhcomp.rs
    ../src/lexical/bignum.rs
    ../src/lexical/cached.rs
    ../src/lexical/cached_float80.rs
    ../src/lexical/digit.rs
    ../src/lexical/errors.rs
    ../src/lexical/exponent.rs
    ../src/lexical/float.rs
    ../src/lexical/large_powers.rs
    ../src/lexical/large_powers32.rs
    ../src/lexical/large_powers64.rs
    ../src/lexical/math.rs
    ../src/lexical/mod.rs
    ../src/lexical/num.rs
    ../src/lexical/parse.rs
    ../src/lexical/rounding.rs
    ../src/lexical/shift.rs
    ../src/lexical/small_powers.rs
    ../src/lib.rs
    ../src/macros.rs
    ../src/map.rs
    ../src/number.rs
    ../src/raw.rs
    ../src/read.rs
    ../src/ser.rs
    ../src/value/de.rs
    ../src/value/from.rs
    ../src/value/index.rs
    ../src/value/mod.rs
    ../src/value/partial_eq.rs
    ../src/value/ser.rs
)

CRATE_INPUTS(
    ../CONTRIBUTING.md
    ../Cargo.lock
    ../LICENSE-APACHE
    ../LICENSE-MIT
    ../README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

LICENSE("MIT OR Apache-2.0")
VERSION(1.0.154)
ORIGINAL_SOURCE(https://static.crates.io/crates/serde_json/serde_json-1.0.154.crate)

CRATE_FEATURES(
    arbitrary_precision
    float_roundtrip
    indexmap
    preserve_order
    raw_value
    unbounded_depth
)

END()
