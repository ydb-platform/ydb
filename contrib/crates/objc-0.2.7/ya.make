# Native Rust build POC; dependencies and features follow Cargo.lock.

CRATE(objc)

CRATE_VERSION(0.2.7)
CRATE_EDITION(2015)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)
VERSION(0.2.7)
LICENSE("MIT")

CRATE_SOURCES(
    examples/example.rs
    src/declare.rs
    src/encode.rs
    src/exception.rs
    src/macros.rs
    src/message/apple/arm.rs
    src/message/apple/arm64.rs
    src/message/apple/mod.rs
    src/message/apple/x86.rs
    src/message/apple/x86_64.rs
    src/message/gnustep.rs
    src/message/mod.rs
    src/message/verify.rs
    src/rc/autorelease.rs
    src/rc/mod.rs
    src/rc/strong.rs
    src/rc/weak.rs
    src/runtime.rs
    src/test_utils.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE.txt
    README.md
)

CRATE_FLAGS(--cap-lints=allow)


PEERDIR(
    contrib/crates/malloc_buf-0.0.6
)

END()
