# WASM service transport and module contracts

This layer combines the coroutine/async network harness with a service-module
SDK, manifest validation and example modules. This layer does not change the
shared WASM ABI or synchronous UDF execution. It is not wired into SQL, DQ or
the FQ control plane.

## Native clients

`TTransport` implements the async runtime's `ITransport` interface. Owners
share a `TTransportExecutor` created with their actor system and user-pool ID.
The executor uses one actor on that pool. HTTP socket readiness arrives through
one shared OS socket waiter per executor; its thread only publishes notifications.
The actor runs `curl_multi_socket_action`, prepares requests and processes
responses on the user pool. gRPC uses an executor-owned completion queue with one shared
waiter. Completion tags likewise only publish results
and wake the actor. They do not resume WASM or process responses on the native
waiter/callback thread. No transform creates its own transport thread. The gRPC
queue outlives its channels, so releasing the last transport from a completion
tag cannot shut down and join the thread executing that tag.
While requests wait for network data, the user-pool thread is available for other work.

Each actor turn processes at most 32 ready transport sessions before yielding.
There is no periodic scan of pending requests. One-shot timers cover concrete
request deadlines, libcurl timeouts and native timer operations. Owner completion
runs on the user pool; only the owner polls/resumes WASM. Destroying a transport
requests cancellation without waiting for network completion. The executor retains
requests until physical cleanup. Producers must stop before actor-system
destruction; final teardown closes the executor, cancels requests and joins the
shared socket waiter after user workers have stopped. It then shuts down and
drains the shared gRPC completion queue before joining its waiter. Tags keep
requests and reservations alive through physical completion. Ordinary query
cancellation does not join either waiter or block the user pool on network cleanup.
A deterministic shutdown regression holds native completion until it is the
last transport owner, then verifies safe destruction and release of credits;
with the old channel-owned callback queue it aborts on a self-join.

Bindings are immutable, trusted, already-resolved inputs from native code.
The guest supplies a binding index, opaque request bytes and the HTTP method/path
or gRPC method. Endpoint authority, credentials and TLS remain host-owned. HTTP
checks TLS certificates/hostnames, optionally accepts a CA file, disables
redirects and ambient proxies, and bounds decoded response bodies. gRPC uses
the guest's method path, injected channel credentials and native metadata.
There are no application retries; gRPC retries are disabled on the channel.
HTTP status and gRPC status codes accompany opaque response bytes. Network
errors are ordinary operation failures, not traps or compartment poisoning.
The private wire-v3 response envelope classifies transport, TLS, authentication,
deadline, cancellation, HTTP/gRPC status and resource-limit failures using enum
and numeric codes only; it does not return backend error strings.

The existing `IHTTPGateway` supports common HTTP verbs, but its upload/delete and
buffered download API lacks a uniform per-request cancellation handle and
deadline; only its streaming download exposes cancellation. The actor HTTP
proxy supports verbs, TLS and connection reuse, but would require an actor
adapter and a bounded decoded-response/cancellation contract. This isolated
prototype uses the existing curl library directly without changing either
shared API. Choosing a production HTTP integration remains a separate gate.

The shared executor defaults to 1024 operations and 256 MiB of reserved buffers;
each transport defaults to 128 operations and 64 MiB. Admission must fit both
sets of limits and otherwise fails immediately with `ResourceLimit`. Pending
timers and cancellations still undergoing native cleanup consume credits.
The byte limits account for conservative owned request/response/header buffer
reservations, not process RSS, library caches or WASM memory.

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

TLS tests cover trusted and untrusted CAs for gRPC, plus trusted, untrusted and
hostname-mismatch certificates for HTTP. Transport tests also exercise shared
operation/byte quotas, cleanup and progress on a single-thread user pool.

Run from the repository root:

```bash
./ya make --build relwithdebinfo -tA ydb/core/fq/libs/wasm_services/ut
```

The transport harness now uses the checked-in Profile module artifact at
`ydb/udfs/wasm/profile/ut/data/profile.wasm`. It retains the byte-oriented fixture
modes and adds typed Profile JSON/protobuf decoding. Build/regeneration commands
are in `ydb/udfs/wasm/profile/README.md`; Echo has its own module and artifact
under `ydb/udfs/wasm/echo`. The former `wasm_services/fixture` target and
`ut/data/transport_coroutine.wasm` resource are no longer used.

## Service-module contract

`ydb/udfs/wasm/sdk/services` defines the bounded row and network envelopes,
module exports and generated dispatch constants. The author-owned manifest
declares the module name, service ABI version and method schemas. Its generator
validates the contract and emits method constants derived from method names;
the native manifest loader derives the same IDs. There are no manual method IDs
or generated manifests. See `ydb/udfs/wasm/sdk/services/README.md` for the schema
and supported scalar types.

Profile owns its JSON/protobuf payloads, strict decoding and batch correlation.
Echo demonstrates a second module with two methods and different row types.
These modules and manifest checks can be tested independently of SQL execution.
SQL examples document the intended integration, which is added in a later stage.

Run the generator checks with:

```bash
./ya make --build relwithdebinfo -tA ydb/udfs/wasm/sdk/services/ut
```

## Remaining Integration Gates

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

## Readiness scheduling

HTTP socket readiness and gRPC completion wake only the affected transport
session. Tests hold HTTP/gRPC responses pending and check that executor dispatches
stop until readiness arrives, completion runs on the user pool and reservations
are released after cleanup. Load-dependent latency validation remains follow-up
work; this is not a production latency guarantee.

Wire v4 (with v3 request compatibility) carries the guest-selected HTTP method and origin-relative path, or gRPC
method. Bindings contain only the remote authority, TLS and host-owned credentials.
Targets cannot override the authority; redirects remain disabled. Target validation
checks framing and syntax, not a per-service method allowlist. Wire v2 is rejected.

HTTP `HEAD` requests finish after response headers, including when Content-Length
describes a nonempty GET representation and the connection remains open.
The native test harness requires empty guest TLS on callback threads, while
allowing the runtime to notify synchronously on its owner during a poll/wait race.

Per-call aliases and the common metric schema are documented in [MONITORING.md](MONITORING.md).
