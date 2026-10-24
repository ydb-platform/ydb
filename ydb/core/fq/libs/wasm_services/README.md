# Experimental FQ WASM service integration

This directory contains service-independent native HTTP/gRPC clients and an
analytical and append-only streaming FQ/DQ output transform. Concrete adapters live in `ydb/udfs/wasm`:
`profile` owns its JSON/protobuf contracts and decoding; `echo` is a second
module with different input/output schemas and two methods. Adding a module
does not require modifying this transform or the coroutine runtime.

## Registration and SQL

`ydbd` obtains service modules from the existing UDF Store. Enable the Store's
WASM loading and compile controller, upload the module and manifest through the
existing UDF API, and enable `WasmServices`. Its default `ModuleSource` is
`UDF_STORE`; this mode accepts no local paths. The common manifest parser accepts
service-only modules and mixed synchronous/service modules. Only synchronous
functions enter the ordinary UDF function registry; service contracts and compiled
bytecode are published in the existing module catalog.

Enable Store loading on each node that prepares or executes FQ queries.
FQ resolves module/method schemas from the current node catalog for each query. A
single generic service transform factory is registered at startup, including
when the catalog is empty. Each new transform acquires the currently loaded
module and libraries by name. It does not download or compile on the user pool.
Modules may therefore arrive or be replaced after server startup.

There is no service-specific version pinning, retained-version store, or
synchronization between preparation and transform creation. An existing
compartment keeps its loaded code; new transforms, restarted queries and different
workers may use a newer module. A missing module or incompatible method/schema
fails normally before network dispatch. ABI and framing checks remain mandatory.

`fqrun` explicitly selects `LOCAL_FILES`, with no UDF Store dependency. For
explicit local-file mode in a server configuration, set `ModuleSource: LOCAL_FILES`
and install the matching files on each compiling/executing node. The manifest
extends the common WASM fields with `service_methods`, as documented in
`ydb/udfs/wasm/sdk/services/README.md`. The author-owned manifest supplies schemas;
the build generates guest dispatch constants, not a separate runtime manifest.
Local modules compile once during registration and require a restart to update.
Both sources use the same compartment, async runtime, HTTP/gRPC transport,
credentials, user pool, limits, deadline and cancellation paths.

This example selects local modules (omit `Modules` and `ModuleSource` to use Store):

```protobuf
WasmServices {
  Enabled: true
  ModuleSource: LOCAL_FILES
  Modules {
    ModulePath: "/absolute/path/profile.wasm"
    ManifestPath: "/absolute/path/profile/manifest.json"
  }
  Modules {
    ModulePath: "/absolute/path/echo.wasm"
    ManifestPath: "/absolute/path/echo/manifest.json"
  }
  MaxBatchRows: 2
  MaxBatchBytes: 32768
  MaxBufferedRows: 65536
  MaxBufferedBytes: 67108864
  MaxTransportOperations: 1024
  MaxTransportBytes: 268435456
  CallTimeoutMs: 30000
}
```

Enable `EXTERNAL_SERVICE` in the existing control-plane storage connection-type
allowlist (`AvailableConnection`). Create connections through the ordinary FQ
`CreateConnection` API; no endpoint or credentials belong in `WasmServices`:

```protobuf
content {
  name: "profiles_http"
  acl { visibility: PRIVATE }
  setting {
    external_service {
      protocol: HTTP
      endpoint: "https://profiles.example.test"
      auth { token { token: "host-owned-token" } }
      headers { key: "Content-Type" value: "application/json" }
      // Optional PEM trust roots, not a path to a file on the FQ server.
      ca_certificate: "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n"
    }
  }
}
```

For gRPC use `protocol: GRPC` and `endpoint: "host:port"`; WASM supplies
`/package.Service/Method` per call. TLS is the default; plaintext requires
`insecure: true` for either protocol. For HTTP the URL scheme must match this
flag. TLS verification cannot be disabled. `auth.none`, `auth.token` and
`auth.current_iam` are supported; TVM uses the separate `tvm_auth` field.
Service-account auth is explicitly rejected until asynchronous initial token
acquisition is integrated. Authorization is
derived from `auth`, never a user-supplied header override. Extra headers are
sensitive and, like bearer tokens, omitted from public connection responses.

Service and auth modules may declare `required_libraries: ["sdk"]`. UDF Store
loads the dependency through its existing library artifacts. In `LOCAL_FILES`
mode, include the SDK binary and its `module_type: "library"` manifest as another
`Modules` entry. Libraries are linked in manifest order and are not exposed as SQL
services. Missing dependencies fail loading; they are never replaced with an
implicit minimal SDK. Auth libraries are instantiated in the private auth realm,
with the same memory limit and without access to application module exports.
Modules with no declared libraries retain the existing minimal-runtime behavior.

`ModulePath` is retained as a single-module configuration shorthand: it requires
an adjacent `<ModulePath>.manifest.json`. Do not combine it with `Modules`.
Rebuild old modules: service row ABI version 2 derives dispatch IDs from method
names instead of manual manifest IDs. Version 1 manifests/requests are rejected.
The underlying async ABI is unchanged. Profile's SQL spelling is preserved.

```sql
$input = SELECT 42ul AS id UNION ALL SELECT 43ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
    WITH CONNECTION='profiles_http',
         INPUT_TYPE=Struct<id:Uint64>,
         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT id, name, score FROM $profiles;
```

For another module, select its manifest name/method and declared schemas, e.g.
`EXTERNAL FUNCTION('Echo', 'Echo')`; see `ydb/udfs/wasm/echo/query.sql`.
Unknown modules/methods/connections, mismatched row schemas and invalid manifests
fail registration/query resolution before network dispatch.
Execution byte/batch limits are checked for the selected method, so an unused
method with a larger declared row does not disable unrelated modules.

## Ownership, Batching and Limits

Each transform owns a compartment, async runtime and native transport. All
guest entries happen on its actor mailbox; native completions only wake it.
Pending I/O frees the calling thread. Query teardown requests cancellation;
the shared executor retains physical native I/O until cleanup completes. Endpoint authorities, PEM roots and credentials stay on the
host; plans contain module/method/connection identities and guests receive only a
binding index, protocol and bounded row bytes.

The gateway resolves connection names only against the query's FQ connection
snapshot, selected by existing scope/visibility rules. Private/public name
priority is unchanged. Only connections referenced by output transforms are
sent to compute actors through the existing private `SecureParams` channel;
credentials are not added to saved graphs, public AST/plans or guest buffers.
Modifying a connection affects subsequently submitted queries, not an already
running query's snapshot. For streaming, also allow `EXTERNAL_SERVICE` in an
explicit `AvailableStreamingConnection` list; an empty list inherits
`AvailableConnection` according to the existing FQ configuration rule.

`BATCH_SIZE` in SQL overrides the host default `MaxBatchRows` for that invocation.
It must be a constant Int32 in 1..64: one selects scalar mode, larger values
request batching and require method batch support. Without the SQL setting,
`MaxBatchRows` is used (zero means the built-in default of one). The configuration
is a default, not a cap: SQL `BATCH_SIZE=64` works with `MaxBatchRows: 1`.
Different invocations of the same connection may select different batch sizes. Cardinality is capped by the method limit,
its maximum input/output row sizes and `MaxBatchBytes` (default 32768, hard cap
32768). The byte limit must fit a request/result header plus one declared row.
For Profile the minimum remains 304 bytes. No timer waits for a full batch;
DQ partitioning/input delivery can produce smaller batches, including one row.

The module owns network payload construction, decoding, domain validation and
correlation. The host validates version, exact cardinality, all field bounds,
UTF-8, booleans and complete result framing before publishing any batch row.
There is one active batch per transform. Deadlines/cancellation cover the
whole call; errors fail execution without publishing partial batch results.
The existing FQ retry policy can restart the whole query and repeat external
calls; a restarted transform acquires the current Store module. Transport itself
does not retry RPCs.
Backpressure retains results and prevents further dispatch. Output row order
follows the module contract; overall SQL order still requires `ORDER BY`.

Buffered input, active rows and undelivered results count toward row quotas.
`MaxBufferedBytes` conservatively reserves the larger declared input/output
row size for each queued row (default 64 MiB). These are serialized-buffer
reservations, not an RSS limit for containers or curl/gRPC/TLS internals.
Native transport additionally reserves request/response copies and releases
leases only after physical request handles/buffers are destroyed.

All transforms registered on the FQ node share one transport executor actor on
`appData->UserPoolId`. HTTP socket readiness wakes this actor through one shared
OS socket waiter per executor. That waiter's thread only publishes notifications;
`curl_multi_socket_action`, request preparation and response processing run on
the user pool. gRPC uses an executor-owned completion queue with one shared
waiter. Completion tags and TVM ticket callbacks likewise only publish results
and wake the actor. They do not resume WASM or process responses on the native
waiter/callback thread. No transform creates its own transport thread. The gRPC
queue outlives its channels, so releasing the last transport from a completion
tag cannot shut down and join the thread executing that tag.

The executor processes at most 32 ready sessions per turn before yielding. It
does not periodically scan pending requests. One-shot timers cover concrete
request deadlines, libcurl timeouts and native timer operations; delayed mock
TVM tickets use a separate one-shot actor timer. Pending I/O leaves the user-pool
thread available for other work. Response processing and owner notifications
remain on the user pool.

`MaxTransportOperations` and `MaxTransportBytes` default to 1024 operations and
256 MiB of conservative buffer reservations across these transports. Individual
transports also retain limits of 128 operations and 64 MiB. Both sets of limits
must admit a request; exhaustion returns `ResourceLimit` immediately. Pending
timers, TVM-ticket waits and operations still undergoing cancellation consume
credits. Reservations are released after native buffers and handles have been destroyed. These are
separate from `MaxBufferedBytes` and WASM memory and are not a process RSS limit.
Final actor-system teardown closes the executor, cancels requests and joins the
shared socket waiter after user workers have stopped. It then shuts down and
drains the shared gRPC completion queue before joining its waiter. Tags keep
requests and reservations alive through physical completion. Ordinary query
cancellation does not join either waiter or block the user pool on network cleanup.
A deterministic shutdown regression holds native completion until it is the
last transport owner, then verifies safe destruction and release of credits;
with the old channel-owned callback queue it aborts on a self-join.

TLS certificates/hostnames are checked; optional PEM trust roots
come from the connection entity. Plaintext requires explicit `insecure: true`.
Redirects, ambient proxies and application/gRPC retries are disabled.
Diagnostics contain fixed text and numeric error classes, never service
payloads, endpoints or credentials. The production HTTP gateway choice remains
open; this prototype uses curl without changing shared HTTP APIs.

## TVM Authorization

Use `external_service.tvm_auth` instead of `external_service.auth` in an FQ
connection to provide source ID, destination ID and the source application's
secret:

```protobuf
tvm_auth {
  source_id: 2020001
  destination_id: 2020002
  secret: "host-owned-source-secret"
}
```

Both IDs must be nonzero uint32 values. TVM cannot be combined with token/IAM
credentials. Secrets are omitted from ordinary public connection reads and
masked in diagnostics. Credentials reach the transport through the existing
private compute channel and never enter guest memory.

`ITvmTicketProvider` in `tvm.h` receives source ID, destination ID, secret and
deadline. Paths and methods are not part of TVM authorization; no TVM method
allowlist is applied. The WASM guest supplies each request method/path independently.
The host adds `X-Ya-Service-Ticket` for HTTP or `x-ya-service-ticket` for gRPC.
Reserved ticket headers cannot be overridden through connection headers.

The default provider is unavailable until the real client/API contract is
integrated. Connections can be created, but execution fails with
`TVM ticket provider is not implemented` before sending to the external service.
There is no unauthenticated fallback. An implementation can be injected into
`RegisterServiceTransforms` and is shared by the node's transforms.

`Request(credentials, deadline, completion)` starts acquisition on the user pool
and returns a cancellable waiter. Its completion callback may run inline or on
another thread and reports `Ready` or `Failed` exactly once. It only publishes
the ticket result and wakes the transport actor; subsequent processing runs on
the user pool. `Request()`, `Cancel()` and waiter destruction must not block.
Cancellation detaches the individual waiter and must tolerate a racing callback.
Acquisition stays subject to the operation's deadline, cancellation, and shared
operation/byte quotas. A real provider must isolate credentials and implement
ticket caching/refresh and independent waiter cancellation.

The test provider returns a fixed fake ticket after a configurable delay using
a one-shot actor timer, without polling, sleeping or creating a thread per ticket.
Single-thread user-pool tests hold one ticket pending while other requests
complete, then fire the controlled timer to finish the first request. Mock tests do not
validate real TVM issuance or ticket signatures. CLI/DDL selection of this mock
is a separate runner integration.

## Verification and Remaining Gates

```bash
./ya make --build relwithdebinfo -tA ydb/core/fq/libs/wasm_services/ut
./ya make --build relwithdebinfo -tA ydb/tests/fq/wasm_services \
    --test-env=YDB_DRIVER_BINARY=/absolute/path/to/installation/ydbd
```

Tests start isolated FQ nodes and local HTTP/gRPC mocks. They leave the normal
installation's config/storage unchanged. The installed binary must point to
the current build. Example-module build/regeneration commands are documented
in `ydb/udfs/wasm/profile/README.md` and `ydb/udfs/wasm/echo/README.md`.

Streaming FQ/DQ v1 uses the same `EXTERNAL FUNCTION` operator with a STREAMING
query and Topic sources/sinks. The transform drains pre-barrier rows asynchronously,
then forwards watermark/checkpoint/finish in order. DQ owns checkpoint storage and
recovery; the transform has no serialized state, compartment, or socket handles.
After recovery the source replays uncheckpointed events into a fresh compartment.
External RPCs may repeat or return different results; the service owns idempotency.
Modules are not pinned: a new transform may load updated code from UDF Store.
Existing call deadlines, backpressure, quotas and error reporting still apply.

`test_streaming.py` covers Topic -> HTTP/gRPC enrichment -> Topic, checkpoints with
pending RPCs and actual compute-process restart. Local fqrun loading stays unchanged.
This does not add KQP/YQv2, gRPC streaming, synchronous robust UDF calls or the
production WASM object ABI. Nested/optional row types, Arrow, parallel batches,
automatic per-call retries and exactly-once external effects are not implemented. Service-account credentials/refresh, connection-aware pooling and production
tenant resource accounting
remain separate gates. Arbitrary service endpoints need an operator-controlled
egress policy before enabling this on shared production tenants. This does not
add KQP external data sources or a network probe to the `TestConnection` API.

### Request routing and readiness

Connection endpoints are HTTP origins or gRPC host:port; they contain no request
method or application path. Wire v4 (with v3 request compatibility) carries the guest's HTTP method/path or gRPC
method. Profile selects its single/batch routes inside WASM. Methods are syntax
checked, with no per-service method authorization list. TVM authorization, when
available, belongs to the connection identity and is independent of the route.

HTTP socket readiness, gRPC completion and TVM ticket completion wake only the
affected transport session. Native tests hold HTTP/gRPC responses pending and
check that executor dispatches stop until readiness arrives, that completion
runs on the user pool and that reservations are released after cleanup.
Load-dependent latency validation remains follow-up work.

HTTP `HEAD` requests finish after response headers, including when Content-Length
describes a nonempty GET representation and the connection remains open.
The native test harness requires empty guest TLS on callback threads, while
allowing the runtime to notify synchronously on its owner during a poll/wait race.

The WASM output transform opts out of DQ channel overcommit. Its advertised byte
budget conservatively reserves one byte per available row because DQ row-size
estimates can be smaller than the service wire encoding. This keeps both row and
output-memory reservations bounded while DQ waits for capacity to become available.

Per-call aliases and the common metric schema are documented in [MONITORING.md](MONITORING.md).
