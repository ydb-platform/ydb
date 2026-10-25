# Native Rust build with dependencies shared through contrib/crates.

CRATE(h2)

CRATE_VERSION(0.4.20)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/client.rs
    src/codec/error.rs
    src/codec/framed_read.rs
    src/codec/framed_write.rs
    src/codec/mod.rs
    src/error.rs
    src/ext.rs
    src/frame/data.rs
    src/frame/go_away.rs
    src/frame/head.rs
    src/frame/headers.rs
    src/frame/mod.rs
    src/frame/ping.rs
    src/frame/priority.rs
    src/frame/reason.rs
    src/frame/reset.rs
    src/frame/settings.rs
    src/frame/stream_id.rs
    src/frame/util.rs
    src/frame/window_update.rs
    src/fuzz_bridge.rs
    src/hpack/decoder.rs
    src/hpack/encoder.rs
    src/hpack/header.rs
    src/hpack/huffman/decode.rs
    src/hpack/huffman/encode.rs
    src/hpack/huffman/mod.rs
    src/hpack/huffman/table.rs
    src/hpack/mod.rs
    src/hpack/table.rs
    src/hpack/test/fixture.rs
    src/hpack/test/fuzz.rs
    src/hpack/test/mod.rs
    src/proto/connection.rs
    src/proto/error.rs
    src/proto/go_away.rs
    src/proto/mod.rs
    src/proto/peer.rs
    src/proto/ping_pong.rs
    src/proto/settings.rs
    src/proto/streams/buffer.rs
    src/proto/streams/counts.rs
    src/proto/streams/flow_control.rs
    src/proto/streams/mod.rs
    src/proto/streams/prioritize.rs
    src/proto/streams/recv.rs
    src/proto/streams/send.rs
    src/proto/streams/state.rs
    src/proto/streams/store.rs
    src/proto/streams/stream.rs
    src/proto/streams/streams.rs
    src/server.rs
    src/share.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    CONTRIBUTING.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/atomic-waker-1.1.2
    contrib/crates/bytes-1.12.1
    contrib/crates/fnv-1.0.7
    contrib/crates/futures-core-0.3.34
    contrib/crates/futures-sink-0.3.34
    contrib/crates/http-1.5.0
    contrib/crates/indexmap-2.14.2
    contrib/crates/slab-0.4.12
    contrib/crates/tokio-1.54.1
    contrib/crates/tokio-util-0.7.20
    contrib/crates/tracing-0.1.44
)

LICENSE("MIT")
VERSION(0.4.20)
ORIGINAL_SOURCE(https://static.crates.io/crates/h2/h2-0.4.20.crate)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
)

END()
