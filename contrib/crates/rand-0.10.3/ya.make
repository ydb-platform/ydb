# Native Rust build with dependencies shared through contrib/crates.

CRATE(rand)

CRATE_VERSION(0.10.3)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/distr/bernoulli.rs
    src/distr/distribution.rs
    src/distr/float.rs
    src/distr/integer.rs
    src/distr/mod.rs
    src/distr/other.rs
    src/distr/slice.rs
    src/distr/uniform.rs
    src/distr/uniform_float.rs
    src/distr/uniform_int.rs
    src/distr/uniform_other.rs
    src/distr/utils.rs
    src/distr/weighted/mod.rs
    src/distr/weighted/weighted_index.rs
    src/prelude.rs
    src/rng.rs
    src/rngs/mod.rs
    src/rngs/small.rs
    src/rngs/std.rs
    src/rngs/thread.rs
    src/rngs/xoshiro128plusplus.rs
    src/rngs/xoshiro256plusplus.rs
    src/seq/coin_flipper.rs
    src/seq/increasing_uniform.rs
    src/seq/index.rs
    src/seq/iterator.rs
    src/seq/mod.rs
    src/seq/slice.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    COPYRIGHT
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    alloc
    default
    std
    std_rng
    sys_rng
    thread_rng
)

PEERDIR(
    contrib/crates/chacha20-0.10.2
    contrib/crates/getrandom-0.4.3
    contrib/crates/rand_core-0.10.1
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.10.3)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand/rand-0.10.3.crate)

END()
