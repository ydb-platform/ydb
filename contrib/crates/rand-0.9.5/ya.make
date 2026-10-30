# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(rand)

CRATE_VERSION(0.9.5)
CRATE_EDITION(2021)
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
    src/rngs/mock.rs
    src/rngs/mod.rs
    src/rngs/reseeding.rs
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
    .cargo-checksum.json
    .cargo_vcs_info.json
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
    os_rng
    small_rng
    std
    std_rng
    thread_rng
)

PEERDIR(
    contrib/crates/rand_chacha-0.9.0
    contrib/crates/rand_core-0.9.5
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.9.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand/rand-0.9.5.crate)

CRATE_FEATURES(
    default
)

CRATE_SOURCES(
    src/distr/uniform_float.rs
    src/distr/uniform_int.rs
    src/distr/uniform_other.rs
    src/seq/index.rs
)

CRATE_SOURCES(
    src/distr/bernoulli.rs
    src/distr/distribution.rs
    src/distr/float.rs
    src/distr/integer.rs
    src/distr/mod.rs
    src/distr/other.rs
    src/distr/slice.rs
    src/distr/uniform.rs
    src/distr/utils.rs
    src/distr/weighted/mod.rs
    src/distr/weighted/weighted_index.rs
    src/lib.rs
    src/prelude.rs
    src/rng.rs
    src/rngs/mock.rs
    src/rngs/mod.rs
    src/rngs/reseeding.rs
    src/rngs/small.rs
    src/rngs/std.rs
    src/rngs/thread.rs
    src/rngs/xoshiro128plusplus.rs
    src/rngs/xoshiro256plusplus.rs
    src/seq/coin_flipper.rs
    src/seq/increasing_uniform.rs
    src/seq/iterator.rs
    src/seq/mod.rs
    src/seq/slice.rs
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
