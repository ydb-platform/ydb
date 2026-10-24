# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(crossbeam_channel)

CRATE_VERSION(0.5.16)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/channel.rs
    src/context.rs
    src/counter.rs
    src/err.rs
    src/flavors/array.rs
    src/flavors/at.rs
    src/flavors/list.rs
    src/flavors/mod.rs
    src/flavors/never.rs
    src/flavors/tick.rs
    src/flavors/zero.rs
    src/select.rs
    src/select_macro.rs
    src/utils.rs
    src/waker.rs
)

CRATE_INPUTS(
    .cargo-checksum.json
    .cargo_vcs_info.json
    CHANGELOG.md
    Cargo.lock
    LICENSE-APACHE
    LICENSE-MIT
    LICENSE-THIRD-PARTY
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    default
    std
)

PEERDIR(
    contrib/crates/crossbeam-utils-0.8.22
)

LICENSE("MIT OR Apache-2.0")
VERSION(0.5.16)
ORIGINAL_SOURCE(https://static.crates.io/crates/crossbeam-channel/crossbeam-channel-0.5.16.crate)

END()
