# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(rand)

CRATE_VERSION(0.8.5)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.8.5)
LICENSE("MIT OR Apache-2.0")

CRATE_SOURCES(
    src/distributions/bernoulli.rs
    src/distributions/distribution.rs
    src/distributions/float.rs
    src/distributions/integer.rs
    src/distributions/mod.rs
    src/distributions/other.rs
    src/distributions/slice.rs
    src/distributions/uniform.rs
    src/distributions/utils.rs
    src/distributions/weighted.rs
    src/distributions/weighted_index.rs
    src/prelude.rs
    src/rng.rs
    src/rngs/adapter/mod.rs
    src/rngs/adapter/read.rs
    src/rngs/adapter/reseeding.rs
    src/rngs/mock.rs
    src/rngs/mod.rs
    src/rngs/small.rs
    src/rngs/std.rs
    src/rngs/thread.rs
    src/rngs/xoshiro128plusplus.rs
    src/rngs/xoshiro256plusplus.rs
    src/seq/index.rs
    src/seq/mod.rs
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


CRATE_FEATURES(small_rng)
PEERDIR(contrib/crates/rand_core-0.6.4)

END()
