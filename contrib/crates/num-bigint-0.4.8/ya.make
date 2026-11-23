# Native Rust build with dependencies shared through contrib/crates.

CRATE(num_bigint)

CRATE_VERSION(0.4.8)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/big_digit.rs
    src/bigint.rs
    src/bigint/addition.rs
    src/bigint/arbitrary.rs
    src/bigint/bits.rs
    src/bigint/convert.rs
    src/bigint/division.rs
    src/bigint/multiplication.rs
    src/bigint/power.rs
    src/bigint/serde.rs
    src/bigint/shift.rs
    src/bigint/subtraction.rs
    src/bigrand.rs
    src/biguint.rs
    src/biguint/addition.rs
    src/biguint/arbitrary.rs
    src/biguint/bits.rs
    src/biguint/convert.rs
    src/biguint/division.rs
    src/biguint/iter.rs
    src/biguint/monty.rs
    src/biguint/multiplication.rs
    src/biguint/power.rs
    src/biguint/serde.rs
    src/biguint/shift.rs
    src/biguint/subtraction.rs
    src/macros.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
    RELEASES.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/num-integer-0.1.47
    contrib/crates/num-traits-0.2.19
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.4.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/num-bigint/num-bigint-0.4.8.crate)

END()
