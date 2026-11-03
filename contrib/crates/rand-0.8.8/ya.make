# Native Rust build with dependencies shared through contrib/crates.

CRATE(rand)

CRATE_VERSION(0.8.8)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

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
    getrandom
    libc
    rand_chacha
    std
    std_rng
)

PEERDIR(
    contrib/crates/rand_chacha-0.3.1
    contrib/crates/rand_core-0.6.4
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
ENDIF()

LICENSE("MIT OR Apache-2.0")
VERSION(0.8.8)
ORIGINAL_SOURCE(https://static.crates.io/crates/rand/rand-0.8.8.crate)

CRATE_FEATURES(
    small_rng
)

CRATE_PROC_MACROS(
    contrib/crates/zerocopy-derive-0.8.63
)

END()
