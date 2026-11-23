# Native Rust build with dependencies shared through contrib/crates.

CRATE(hyper)

CRATE_VERSION(1.12.0)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/body/chan.rs
    src/body/incoming.rs
    src/body/length.rs
    src/body/mod.rs
    src/cfg.rs
    src/client/conn/http1.rs
    src/client/conn/http2.rs
    src/client/conn/mod.rs
    src/client/dispatch.rs
    src/client/mod.rs
    src/client/tests.rs
    src/common/buf.rs
    src/common/date.rs
    src/common/either.rs
    src/common/future.rs
    src/common/io/compat.rs
    src/common/io/mod.rs
    src/common/io/rewind.rs
    src/common/lock.rs
    src/common/mod.rs
    src/common/task.rs
    src/common/time.rs
    src/error.rs
    src/ext/h1_reason_phrase.rs
    src/ext/informational.rs
    src/ext/mod.rs
    src/ffi/body.rs
    src/ffi/client.rs
    src/ffi/error.rs
    src/ffi/http_types.rs
    src/ffi/io.rs
    src/ffi/macros.rs
    src/ffi/mod.rs
    src/ffi/task.rs
    src/headers.rs
    src/mock.rs
    src/proto/h1/conn.rs
    src/proto/h1/decode.rs
    src/proto/h1/dispatch.rs
    src/proto/h1/encode.rs
    src/proto/h1/io.rs
    src/proto/h1/mod.rs
    src/proto/h1/role.rs
    src/proto/h2/client.rs
    src/proto/h2/mod.rs
    src/proto/h2/ping.rs
    src/proto/h2/server.rs
    src/proto/h2/upgrade.rs
    src/proto/mod.rs
    src/rt/bounds.rs
    src/rt/io.rs
    src/rt/mod.rs
    src/rt/timer.rs
    src/server/conn/http1.rs
    src/server/conn/http2.rs
    src/server/conn/mod.rs
    src/server/mod.rs
    src/service/http.rs
    src/service/mod.rs
    src/service/service.rs
    src/service/util.rs
    src/trace.rs
    src/upgrade.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    client
    default
    http1
    http2
    server
)

PEERDIR(
    contrib/crates/atomic-waker-1.1.2
    contrib/crates/bytes-1.12.1
    contrib/crates/futures-channel-0.3.34
    contrib/crates/futures-core-0.3.34
    contrib/crates/h2-0.4.20
    contrib/crates/http-1.5.0
    contrib/crates/http-body-1.1.0
    contrib/crates/httparse-1.10.1
    contrib/crates/httpdate-1.0.3
    contrib/crates/itoa-1.0.18
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/smallvec-1.16.4
    contrib/crates/tokio-1.54.1
    contrib/crates/want-0.3.2
)

LICENSE("MIT")
VERSION(1.12.0)
ORIGINAL_SOURCE(https://static.crates.io/crates/hyper/hyper-1.12.0.crate)

CRATE_FEATURES(
    full
)

CRATE_PROC_MACROS(
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
)

END()
