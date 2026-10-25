# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(http_body_util)

CRATE_VERSION(0.1.4)
CRATE_EDITION(2018)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/channel.rs
    src/collected.rs
    src/combinators/box_body.rs
    src/combinators/collect.rs
    src/combinators/frame.rs
    src/combinators/fuse.rs
    src/combinators/inspect_err.rs
    src/combinators/inspect_frame.rs
    src/combinators/map_err.rs
    src/combinators/map_frame.rs
    src/combinators/mod.rs
    src/combinators/with_trailers.rs
    src/either.rs
    src/empty.rs
    src/full.rs
    src/limited.rs
    src/stream.rs
    src/util.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/futures-core-0.3.33
    contrib/crates/http-1.4.2
    contrib/crates/http-body-1.1.0
    contrib/crates/pin-project-lite-0.2.17
)

LICENSE("MIT")
VERSION(0.1.4)
ORIGINAL_SOURCE(https://static.crates.io/crates/http-body-util/http-body-util-0.1.4.crate)

END()
