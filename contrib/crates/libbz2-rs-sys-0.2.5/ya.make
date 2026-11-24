# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(libbz2_rs_sys)

CRATE_VERSION(0.2.5)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/allocator.rs
    src/blocksort.rs
    src/bzlib.rs
    src/compress.rs
    src/crctable.rs
    src/decompress.rs
    src/high_level.rs
    src/huffman.rs
    src/randtable.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    Cargo.lock
    LICENSE
    README.md
    include/bzlib.h
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    rust-allocator
)

LICENSE("bzip2-1.0.6")
VERSION(0.2.5)
ORIGINAL_SOURCE(https://static.crates.io/crates/libbz2-rs-sys/libbz2-rs-sys-0.2.5.crate)

CRATE_SOURCES(
    src/allocator.rs
    src/blocksort.rs
    src/bzlib.rs
    src/compress.rs
    src/crctable.rs
    src/decompress.rs
    src/high_level.rs
    src/huffman.rs
    src/lib.rs
    src/randtable.rs
)

END()
