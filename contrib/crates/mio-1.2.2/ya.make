# Rust dependency of temporalio 1.32.0; versions follow its Cargo.lock.

CRATE(mio)

CRATE_VERSION(1.2.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/event/event.rs
    src/event/events.rs
    src/event/mod.rs
    src/event/source.rs
    src/interest.rs
    src/io_source.rs
    src/macros.rs
    src/net/mod.rs
    src/net/tcp/listener.rs
    src/net/tcp/mod.rs
    src/net/tcp/stream.rs
    src/net/udp.rs
    src/net/uds/datagram.rs
    src/net/uds/listener.rs
    src/net/uds/mod.rs
    src/net/uds/stream.rs
    src/poll.rs
    src/sys/mod.rs
    src/sys/shell/mod.rs
    src/sys/shell/selector.rs
    src/sys/shell/tcp.rs
    src/sys/shell/udp.rs
    src/sys/shell/uds.rs
    src/sys/shell/waker.rs
    src/sys/unix/mod.rs
    src/sys/unix/net.rs
    src/sys/unix/pipe.rs
    src/sys/unix/selector/epoll.rs
    src/sys/unix/selector/event_ports.rs
    src/sys/unix/selector/kqueue.rs
    src/sys/unix/selector/poll.rs
    src/sys/unix/selector/stateless_io_source.rs
    src/sys/unix/sourcefd.rs
    src/sys/unix/tcp.rs
    src/sys/unix/udp.rs
    src/sys/unix/uds/datagram.rs
    src/sys/unix/uds/listener.rs
    src/sys/unix/uds/mod.rs
    src/sys/unix/uds/stream.rs
    src/sys/unix/waker/event_ports.rs
    src/sys/unix/waker/eventfd.rs
    src/sys/unix/waker/kqueue.rs
    src/sys/unix/waker/pipe.rs
    src/sys/unix/waker/single_threaded.rs
    src/sys/wasip1/mod.rs
    src/sys/windows/afd.rs
    src/sys/windows/event.rs
    src/sys/windows/handle.rs
    src/sys/windows/io_status_block.rs
    src/sys/windows/iocp.rs
    src/sys/windows/mod.rs
    src/sys/windows/named_pipe.rs
    src/sys/windows/net.rs
    src/sys/windows/overlapped.rs
    src/sys/windows/selector.rs
    src/sys/windows/tcp.rs
    src/sys/windows/udp.rs
    src/sys/windows/waker.rs
    src/token.rs
    src/waker.rs
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
    net
    os-ext
    os-poll
)

IF (_RUST_TARGET == "aarch64-apple-darwin" OR _RUST_TARGET == "aarch64-unknown-linux-gnu" OR _RUST_TARGET == "aarch64-unknown-linux-musl" OR _RUST_TARGET == "x86_64-apple-darwin" OR _RUST_TARGET == "x86_64-unknown-linux-gnu" OR _RUST_TARGET == "x86_64-unknown-linux-musl")
    PEERDIR(
        contrib/crates/libc-0.2.189
    )

ELSEIF (_RUST_TARGET == "x86_64-pc-windows-msvc")
    PEERDIR(
        contrib/crates/windows-sys-0.61.2
    )

ENDIF()

LICENSE("MIT")
VERSION(1.2.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/mio/mio-1.2.2.crate)

END()
