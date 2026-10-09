# FQ WASM service transport prototype (P2)

This is a network harness for the coroutine/async bridge from P1. All P2 code
lives under `ydb/core/fq`; it does not change the shared WASM ABI or synchronous
UDF execution. The opt-in `query` integration connects the private Profile
example to analytical FQ queries through an asynchronous DQ output transform.

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
hostname-mismatch certificates for HTTP. These are private prototype contracts,
not a production connection schema.

Run from the repository root:

```bash
./ya make --build relwithdebinfo -tA ydb/core/fq/libs/wasm_services/ut
```

The checked-in WASM resource is generated from `fixture/main.cpp`. Regenerate
it with the current supported toolchain and rerun the native tests:

```bash
./ya make --build relwithdebinfo contrib/tools/protoc contrib/libs/nanopb/generator
contrib/tools/protoc/protoc -I ydb/core/fq/libs/wasm_services/ut/protos \
    --plugin=protoc-gen-nanopb="$PWD/contrib/libs/nanopb/generator/generator" \
    --nanopb_opt=-fydb/core/fq/libs/wasm_services/ut/protos/profile.options \
    --nanopb_out=ydb/core/fq/libs/wasm_services/fixture/proto profile.proto
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
- Generalize the experimental Profile SQL/DQ integration to object methods,
  row-driven arguments, parallel calls and a production connection schema.

`wire.h` is an internal, little-endian harness envelope, not a new public
connection schema, DDL or typed YQL ABI. Test bindings are not a substitute
for metadata/ACL validation.

## Experimental FQ Query Integration

The `query` library uses the existing SQL `PROCESS ... EXTERNAL FUNCTION`
provider and DQ output-transform interface. It is available only for analytical
queries on the FQ DQ compute path (v1), not KQP/YQv2 or streaming/checkpoints.
Each transform owns a WASM compartment and native transport. Calls are processed
one at a time, with a bounded input queue (65536 IDs by default) and one pending
batch of typed results. A full downstream buffer stops further network dispatch.
Native I/O only wakes the actor; the actor enters WASM on its own mailbox. Query teardown
cancels the runtime and drains native I/O. `CallTimeoutMs` sets a per-call wall
deadline (30000 by default); the enclosing FQ query deadline cancels the actor.

Add an operator-owned `WasmServices` section to the federated query configuration
on every FQ node that compiles or executes the query:

```protobuf
WasmServices {
  Enabled: true
  ModulePath: "/absolute/path/transport_coroutine.wasm"
  CallTimeoutMs: 30000
  MaxBufferedRows: 65536
  Bindings {
    Alias: "profiles_http"
    Protocol: HTTP
    Endpoint: "https://profiles.example.test/profile"
    Method: "POST"
    CaFile: "/absolute/path/ca.pem"
    Headers { key: "Content-Type" value: "application/json" }
    Headers { key: "Authorization" value: "Bearer operator-owned-token" }
  }
  Bindings {
    Alias: "profiles_grpc"
    Protocol: GRPC
    Endpoint: "profiles.example.test:443"
    Method: "/NFq.NWasmServices.NTest.MockService/Lookup"
    CaFile: "/absolute/path/ca.pem"
    Headers { key: "authorization" value: "Bearer operator-owned-token" }
  }
}
```

Use the module generated from `fixture/main.cpp` above. The operator selects the
module and installs the same aliases/module on all participating nodes. The
module is compiled at factory registration and instantiated per transform.
`GrpcInsecure: true` is an explicit plaintext opt-in for local mock services;
otherwise gRPC uses TLS. HTTP retains transport certificate/hostname checks.
Endpoints, CA paths and headers never enter the query plan or guest arguments:
the plan carries the alias only. Enabling this prototype grants analytical query
users access to the configured aliases; metadata ACLs and credential refresh
are still pending. Keep it disabled on shared production tenants.

Submit this SQL as an analytical query through the usual FQ API:

```sql
$input = SELECT 42ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
    WITH CONNECTION='profiles_http',
         INPUT_TYPE=Struct<id:Uint64>,
         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT id, name, score FROM $profiles;
```

Change the connection to `profiles_grpc` for protobuf/gRPC. The private request
and response formats are defined in `fixture/main.cpp` and `ut/protos/profile.proto`.
Unknown aliases, wrong row schemas, service errors, invalid payloads and deadline
expiration fail the query. Diagnostics contain fixed messages and numeric error
classes, not response bodies, endpoints or credentials. Batching is explicitly
enabled as below. There are no parallel-call, automatic retry or replay
guarantees in either mode.

### Batching

`MaxBatchRows: 0` or `1` preserves the existing single-row protocol (the default).
Set `MaxBatchRows` to `2..64` to enable the private batch protocol on all bindings.
Configure batch-capable HTTP endpoints and gRPC methods; a scalar service cannot
be converted to a batch API by the host. The SQL and row schemas stay unchanged.

```protobuf
MaxBatchRows: 32
MaxBatchBytes: 32768
```

The HTTP request is `{"ids":[42,43]}` and the response is an ordered JSON array
of Profile objects. The gRPC method uses `ProfileBatchRequest` and
`ProfileBatchReply`, whose opaque payload contains `ProfileBatchPayload`.
For the mocks, use `/profile/batch` and
`/NFq.NWasmServices.NTest.MockService/LookupBatch` respectively.

The transform takes up to the configured number of already available rows;
there is no timer waiting for a full batch. Partial batches, including the final
single row, use the batch protocol. Exactly one RPC is sent per batch. The
maximum is a cap, not a guarantee of full batches; DQ input delivery and task
partitioning can produce smaller groups. There is one active batch per transform.

`MaxBatchBytes` defaults to 32768 and must be between 304 and 32768. It caps each
application request/response and the guest result, excluding native transport
framing. Batch cardinality is conservatively reduced to fit the typed guest
result header and fixed-size result items. Native clients bound response buffers
by this cap, and the guest separately checks framing and decoded data.

Responses must contain exactly one result per input position, in the same order,
with the corresponding `id`; repeated IDs are allowed and are not deduplicated.
All items are validated before any row from that batch is published. A malformed,
missing, extra, reordered, invalid or oversized item/response fails the query;
there are no partial-success semantics. Deadlines and cancellation cover the
whole active batch. Buffered input, active IDs and undelivered results all count
toward `MaxBufferedRows`. Backpressure retains undelivered results and prevents
new dispatch. Arrow and parallel batches are not implemented.

The native suite includes output-transform backpressure, typed HTTP/gRPC rows,
deadline, malformed response and in-flight gRPC cancellation. Full FQ API tests:

```bash
./ya make --build relwithdebinfo -tA ydb/tests/fq/wasm_services
```

To run these checks through an installed `ydbd` linked to the build output:

```bash
./ya make --build relwithdebinfo -tA ydb/tests/fq/wasm_services \
    --test-env=YDB_DRIVER_BINARY=/absolute/path/to/installation/ydbd
```

The tests start isolated FQ control/compute nodes and HTTP/gRPC mocks with
temporary configs and storage; they do not modify the installation's data or
its normal server config. The installation's binary must point to this build,
not to an older release without the experimental config field.
