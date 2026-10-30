# Native Rust build with dependencies shared through contrib/crates.

CRATE(hyper_util)

CRATE_VERSION(0.1.21)
CRATE_EDITION(2024)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/client/legacy/client.rs
    src/client/legacy/connect/capture.rs
    src/client/legacy/connect/dns.rs
    src/client/legacy/connect/http.rs
    src/client/legacy/connect/mod.rs
    src/client/legacy/connect/proxy/mod.rs
    src/client/legacy/connect/proxy/socks/mod.rs
    src/client/legacy/connect/proxy/socks/v4/errors.rs
    src/client/legacy/connect/proxy/socks/v4/messages.rs
    src/client/legacy/connect/proxy/socks/v4/mod.rs
    src/client/legacy/connect/proxy/socks/v5/errors.rs
    src/client/legacy/connect/proxy/socks/v5/messages.rs
    src/client/legacy/connect/proxy/socks/v5/mod.rs
    src/client/legacy/connect/proxy/tunnel.rs
    src/client/legacy/mod.rs
    src/client/legacy/pool.rs
    src/client/mod.rs
    src/client/pool/cache.rs
    src/client/pool/map.rs
    src/client/pool/mod.rs
    src/client/pool/negotiate.rs
    src/client/pool/singleton.rs
    src/client/proxy/matcher.rs
    src/client/proxy/mod.rs
    src/common/exec.rs
    src/common/lazy.rs
    src/common/mod.rs
    src/common/rewind.rs
    src/common/sync.rs
    src/common/timer.rs
    src/rt/io.rs
    src/rt/mod.rs
    src/rt/tokio.rs
    src/rt/tokio/with_hyper_io.rs
    src/rt/tokio/with_tokio_io.rs
    src/rt/tracing.rs
    src/server/conn/auto/mod.rs
    src/server/conn/auto/upgrade.rs
    src/server/conn/mod.rs
    src/server/graceful.rs
    src/server/mod.rs
    src/service/glue.rs
    src/service/mod.rs
    src/service/oneshot.rs
)

CRATE_INPUTS(
    CHANGELOG.md
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

CRATE_FEATURES(
    client
    client-legacy
    client-proxy
    default
    http1
    http2
    server
    server-auto
    service
    tokio
)

PEERDIR(
    contrib/crates/base64-0.23.1
    contrib/crates/bytes-1.12.1
    contrib/crates/futures-channel-0.3.34
    contrib/crates/futures-util-0.3.34
    contrib/crates/http-1.5.0
    contrib/crates/http-body-1.1.0
    contrib/crates/httparse-1.10.1
    contrib/crates/hyper-1.12.0
    contrib/crates/ipnet-2.12.2
    contrib/crates/libc-0.2.191
    contrib/crates/percent-encoding-2.3.2
    contrib/crates/pin-project-lite-0.2.17
    contrib/crates/socket2-0.6.5
    contrib/crates/tokio-1.54.1
    contrib/crates/tower-service-0.3.3
    contrib/crates/tracing-0.1.44
)

LICENSE("MIT")
VERSION(0.1.21)
ORIGINAL_SOURCE(https://static.crates.io/crates/hyper-util/hyper-util-0.1.21.crate)

CRATE_FEATURES(
    client-proxy-system
)

IF (OS_DARWIN AND ARCH_ARM64)
    PEERDIR(
    contrib/crates/system-configuration-0.7.0
)

ENDIF()

IF (OS_DARWIN AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/system-configuration-0.7.0
)

ENDIF()

IF (OS_WINDOWS AND ARCH_X86_64)
    PEERDIR(
    contrib/crates/windows-registry-0.6.1
)

ENDIF()

CRATE_PROC_MACROS(
    contrib/crates/bytemuck_derive-1.12.1
    contrib/crates/futures-macro-0.3.34
    contrib/crates/serde_derive-1.0.229
    contrib/crates/tokio-macros-2.7.2
    contrib/crates/tracing-attributes-0.1.31
)

END()
