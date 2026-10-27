# Native Rust build with dependencies shared through contrib/crates.

CRATE(zopfli)

CRATE_VERSION(0.8.4)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/blocksplitter.rs
    src/cache.rs
    src/deflate.rs
    src/gzip.rs
    src/hash.rs
    src/io.rs
    src/iter.rs
    src/katajainen.rs
    src/lz77.rs
    src/main.rs
    src/math.rs
    src/squeeze.rs
    src/symbols.rs
    src/tree.rs
    src/util.rs
    src/zlib.rs
)

CRATE_INPUTS(
    CONTRIBUTORS
    COPYING
    Cargo.lock
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    gzip
    std
    zlib
)

PEERDIR(
    contrib/crates/bumpalo-3.20.3
    contrib/crates/crc32fast-1.5.3
    contrib/crates/log-0.4.34
    contrib/crates/simd-adler32-0.3.10
)

LICENSE("Apache-2.0")
VERSION(0.8.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/zopfli/zopfli-0.8.4.crate)

END()
