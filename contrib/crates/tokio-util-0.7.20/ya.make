# Native Rust build with dependencies shared through contrib/crates.

CRATE(tokio_util)

CRATE_VERSION(0.7.20)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/cfg.rs
    src/codec/any_delimiter_codec.rs
    src/codec/bytes_codec.rs
    src/codec/decoder.rs
    src/codec/encoder.rs
    src/codec/framed.rs
    src/codec/framed_impl.rs
    src/codec/framed_read.rs
    src/codec/framed_write.rs
    src/codec/length_delimited.rs
    src/codec/lines_codec.rs
    src/codec/mod.rs
    src/compat.rs
    src/context.rs
    src/either.rs
    src/future.rs
    src/future/with_cancellation_token.rs
    src/io/copy_to_bytes.rs
    src/io/inspect.rs
    src/io/mod.rs
    src/io/read_arc.rs
    src/io/read_buf.rs
    src/io/reader_stream.rs
    src/io/simplex.rs
    src/io/sink_writer.rs
    src/io/stream_reader.rs
    src/io/sync_bridge.rs
    src/io/write_all_vectored.rs
    src/loom.rs
    src/net/mod.rs
    src/net/unix/mod.rs
    src/sync/cancellation_token.rs
    src/sync/cancellation_token/guard.rs
    src/sync/cancellation_token/guard_ref.rs
    src/sync/cancellation_token/tree_node.rs
    src/sync/mod.rs
    src/sync/mpsc.rs
    src/sync/poll_semaphore.rs
    src/sync/reusable_box.rs
    src/task/abort_on_drop.rs
    src/task/join_map.rs
    src/task/join_queue.rs
    src/task/mod.rs
    src/task/spawn_pinned.rs
    src/task/task_tracker.rs
    src/time/delay_queue.rs
    src/time/mod.rs
    src/time/wheel/level.rs
    src/time/wheel/mod.rs
    src/time/wheel/stack.rs
    src/tracing.rs
    src/udp/frame.rs
    src/udp/mod.rs
    src/util/maybe_dangling.rs
    src/util/memchr.rs
    src/util/mod.rs
    src/util/poll_buf.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    codec
    default
    io
    io-util
    libc
)

PEERDIR(
    contrib/crates/bytes-1.12.1
    contrib/crates/futures-core-0.3.34
    contrib/crates/futures-sink-0.3.34
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/tokio-1.54.1
)

IF (OS_DARWIN AND ARCH_ARM64 OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_AARCH64 AND NOT MUSL OR OS_LINUX AND ARCH_AARCH64 AND _RUST_MODE != "host" AND MUSL OR OS_DARWIN AND ARCH_X86_64 OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE == "host" OR OS_LINUX AND ARCH_X86_64 AND NOT MUSL OR OS_LINUX AND ARCH_X86_64 AND _RUST_MODE != "host" AND MUSL)
    PEERDIR(
    contrib/crates/libc-0.2.191
)

ELSEIF (OS_WINDOWS AND ARCH_X86_64)
ENDIF()

LICENSE("MIT")
VERSION(0.7.20)
ORIGINAL_SOURCE(https://static.crates.io/crates/tokio-util/tokio-util-0.7.20.crate)

CRATE_FEATURES(
    compat
    full
    futures-io
    futures-util
    hashbrown
    net
    rt
    slab
    time
)

CRATE_FEATURES(
    join-map
)

PEERDIR(
    contrib/crates/futures-io-0.3.34
    contrib/crates/futures-util-0.3.34
    contrib/crates/hashbrown-0.15.5
    contrib/crates/slab-0.4.12
)

CRATE_SOURCES(
    src/sync/tests/loom_cancellation_token.rs
    src/sync/tests/mod.rs
)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
)

END()
