# FQ WASM service transport prototype (P2)

This is a network harness for the coroutine/async bridge from P1. All P2 code
lives under `ydb/core/fq`; it does not change the shared WASM ABI or synchronous
UDF execution. It is not wired into SQL, DQ or the FQ control plane.

## Native clients

`TTransport` implements the P1 `ITransport` interface. It owns one curl multi
worker for HTTP and timers, and one gRPC completion-queue worker for generic
unary calls. Pending requests do not occupy the calling/owner thread and do
not each allocate a worker thread. Callbacks publish owned result bytes and
notify the owner; only the owner polls/resumes WASM. Destroy the transport on
the owner thread: destruction cancels and drains physical I/O.

Bindings are immutable, trusted, already-resolved inputs from native code.
The guest supplies a binding index and opaque request bytes, never an endpoint,
method, credentials or transport options. HTTP supports GET/POST/PUT/DELETE,
checks TLS certificates/hostnames, optionally accepts a CA file, disables
redirects and ambient proxies, and bounds decoded response bodies. gRPC uses
the binding's method path, injected channel credentials and native metadata.
There are no application retries; gRPC retries are disabled on the channel.
HTTP status and gRPC status codes accompany opaque response bytes. Network
errors are ordinary operation failures, not traps or compartment poisoning.
The private wire-v2 response envelope classifies transport, TLS, authentication,
deadline, cancellation, HTTP/gRPC status and resource-limit failures using enum
and numeric codes only; it does not return backend error strings.

The existing `IHTTPGateway` supports these verbs, but its upload/delete and
buffered download API lacks a uniform per-request cancellation handle and
deadline; only its streaming download exposes cancellation. The actor HTTP
proxy supports verbs, TLS and connection reuse, but would require an actor
adapter and a bounded decoded-response/cancellation contract. This isolated
prototype uses the existing curl library directly without changing either
shared API. Choosing a production HTTP integration remains a separate gate.

Before dispatch, operation and byte quotas reserve request/header storage,
response framing and buffer copies. A lease remains charged until the native
request, response buffers and client handles are physically destroyed, even
after logical cancellation/completion. These counters are conservative
application-buffer reservations, not an RSS limit for curl/gRPC/TLS internals
or shared connection pools. P1 separately accounts owner/guest buffers.

## Tests

Local HTTP and unary gRPC servers record requests and wait for explicit test
commands to reply, fail or disconnect. Fixture setup selects local ports and
teardown stops the servers. All waits have timeouts; tests need no external
services. The WASM64 C++20 fixture uses the P1 SDK and runs single, sequential
and parallel calls. Its reusable, bounded allocator exposes live object counts
to verify coroutine/frame/buffer cleanup. Network tests complement, rather
than replace, P1's deterministic fake-transport race tests.

The fixture also exercises a private typed `Profile` example over HTTP JSON and
gRPC protobuf: bounded UTF-8 names, required fields, numeric ranges, schema
version, malformed payload handling, and sequential/parallel adapter modes.
TLS tests cover trusted and untrusted CAs for gRPC, plus trusted, untrusted and
hostname-mismatch certificates for HTTP. These are test contracts, not a
production-facing connection or YQL type.

Run from the repository root:

```bash
./ya make --build relwithdebinfo -tA ydb/core/fq/libs/wasm_services/ut
```

The checked-in WASM resource is generated from `fixture/main.cpp`. Regenerate
it with the current supported toolchain and rerun the native tests:

```bash
./ya make --build release --target-platform=clang20-emscripten-wasm64 ydb/core/fq/libs/wasm_services/fixture
cp -L ydb/core/fq/libs/wasm_services/fixture/libwasm_services-fixture.so ydb/core/fq/libs/wasm_services/ut/data/transport_coroutine.wasm
chmod 644 ydb/core/fq/libs/wasm_services/ut/data/transport_coroutine.wasm
```

## Remaining P2 Gates

This harness does not establish the full RFC P2 acceptance criteria:

- Resolve aliases through existing YDB/FQ metadata, permissions and secrets;
  test unknown alias, access denial and incompatible connection types.
- Integrate credential refresh and connection identity/version-aware pooling.
- Validate TLS with production identity sources and client authentication;
  current tests use generated local server certificates and trusted CA files.
- Add production worker/tenant resource accounting beyond application-buffer
  reservations and validate diagnostics against production service behavior.
- Connect execution to DQ/SQL through the later RFC stages. No production
  configuration or external-network access is exposed by this prototype.

`wire.h` is an internal, little-endian harness envelope, not a new public
connection schema, DDL or typed YQL ABI. Test bindings are not a substitute
for metadata/ACL validation.
