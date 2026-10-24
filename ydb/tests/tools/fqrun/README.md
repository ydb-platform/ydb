# FQ run tool

Tool can be used to execute analytical and streaming queries using FQ proxy infrastructure.

For profiling memory allocations build fqrun with ya make flags `-D PROFILE_MEMORY_ALLOCATIONS -D CXXFLAGS=-DPROFILE_MEMORY_ALLOCATIONS`.

## Scripts

* `flame_graph.sh` - script for collecting flame graphs in svg format, usage:
    ```(bash)
    ./flame_graph.sh [graph collection time in seconds] [use sudo]
    ```

## Examples

### Queries

* Stream query:
    ```(bash)
    ./fqrun -s "SELECT 42"
    ```

* Analytics query (compute routing comes from the configuration):
    ```(bash)
    ./fqrun -s "SELECT 42" -C analytics
    ```

* Explicit in-place analytics query:
    ```bash
    ./fqrun -s "SELECT 42" -C analytics --compute=in-place
    ```

`--compute=in-place|ydb` overrides the default compute and all scope/query-type
compute mappings after loading `--cfg`, regardless of argument order. Without
it, existing routing is preserved (the bundled config routes analytics to YDB).
`in-place` cannot be combined with `--single-compute-db` or `--shared-compute-db`.

* Analytics query with remote compute database:
    1.  Start shared ydb tenant:
        ```(bash)
        ./kqprun -G 12345 --shared db
        ```

    1. Execute query:
        ```(bash)
        ./fqrun -s "SELECT 42" -C analytics --shared-compute-db /Root/db@localhost:12345
        ```

### Logs

* Setup log settings:
    ```(bash)
    ./fqrun -s "SELECT 42" --log-default=warn --log FQ_RUN_ACTOR=trace --log-file query.log
    ```

### Cluster

* Embedded UI:
    ```(bash)
    ./fqrun -M 32000
    ```

    Monitoring endpoint: http://localhost:32000

* gRPC endpoint:
    ```(bash)
    ./fqrun -G 32000
    ```

    Connect with ydb CLI: `ydb -e grpc://localhost:32000 -d /Root`

## WASM service adapters

The experimental service adapters run in analytical and append-only streaming
FQ/DQ in-place execution. Use `-C analytics --compute=in-place` or `-C stream`;
`async-analytics` and `async-stream` can submit concurrent queries. This is
separate from native UDF loading with `--udf`/`--udfs-dir`. YDB/KQP compute is
unsupported for this integration.

Configure modules and limits in the existing `--cfg` application configuration:

```protobuf
FederatedQueryConfig {
  WasmServices {
    Enabled: true
    Modules {
      ModulePath: "/absolute/path/profile.wasm"
      ManifestPath: "/absolute/path/profile/manifest.json"
    }
    MaxBatchRows: 2
    MaxBatchBytes: 32768
    MaxBufferedRows: 65536
    MaxBufferedBytes: 67108864
    MaxTransportOperations: 1024
    MaxTransportBytes: 268435456
    CallTimeoutMs: 30000
  }
}
```

Add this section to a complete FQ application configuration, such as the bundled
`configuration/app_config.conf`. At startup FQ reads and validates the manifests,
compiles the modules and registers their transforms. Each running transform
gets a separate WASM compartment and async runtime, reusing the compiled module
across rows/batches. Install matching artifacts on every compiling/executing node;
the runner does not distribute modules or hot-reload them. Restart with the new
module and matching manifest to update an adapter.

### Execution and transport limits

Transforms share a transport executor actor on the node's user pool
(`appData->UserPoolId`), created once by `RegisterServiceTransforms`. HTTP uses
one shared OS socket waiter per executor; its thread only publishes readiness
notifications. The actor runs `curl_multi_socket_action`, starts requests and
processes responses on the user pool. gRPC and TVM completion callbacks only
publish their results and wake the actor. WASM resumes through its owning compute
actor when data is ready. No transform creates its own transport thread. While
requests are waiting, the user-pool thread is available to other work.

Each turn processes at most 32 ready transport sessions before yielding to the
actor scheduler. There is no periodic scan of pending requests. One-shot timers
cover concrete request deadlines, libcurl timeouts and native timer operations;
the delayed TVM mock uses a one-shot actor timer. Request preparation, transport
progress, response processing and owner notifications share the user pool's CPU
budget.

`MaxTransportOperations` and `MaxTransportBytes` in the configuration above are
shared limits for all WASM service transports registered on the FQ node. Their
defaults are 1024 operations and 256 MiB. Each individual transport also retains
its limits of 128 operations and 64 MiB. Admission must fit both sets of limits;
otherwise the operation fails immediately with `ResourceLimit`, without adding
it to an unbounded waiting queue. Operations waiting for a TVM ticket or timer,
and operations whose cancellation is still being cleaned up, consume credits.

The byte limit reserves space conservatively for owned request, response and
header buffers, including copies. It is not a hard process RSS limit: library
allocations such as gRPC/TLS caches and WASM runtime memory are outside this
accounting. `MaxBufferedBytes` remains a separate limit on each transform's
buffered data. For example, the configuration above allows at most 256 MiB of
transport reservations across the node, in addition to each transform's own
buffering and WASM memory.

Destroying a transform requests cancellation and does not join transport
threads or wait for network completion. Credits are released only after the
underlying operations have been cleaned up. Final actor-system shutdown closes
the executor, cancels requests and joins the shared socket waiter after user
workers stop. A late gRPC callback retains its request and reservation until
physical completion; ordinary query cancellation does not block the user pool
on network cleanup.

When WASM services are enabled, fqrun adds `EXTERNAL_SERVICE` to the local
connection allowlist and uses real control-plane storage on its embedded YDB
database: the lightweight in-memory implementation does not implement query
cancellation. No external cluster is needed; `--cp-storage-db` can still select
an external metadata database. `--connection` accepts a `FederatedQuery::ConnectionContent`
file directly, without a `content { ... }` wrapper:

```protobuf
name: "profiles_http"
acl { visibility: PRIVATE }
setting {
  external_service {
    protocol: HTTP
    endpoint: "https://profiles.example.test"
    auth { token { token: "${YQL_TOKEN}" } }
  }
}
```

For that placeholder use `--templates` and provide the existing `YQL_TOKEN`
environment variable. TLS is enabled by default; `ca_certificate` accepts PEM
roots. Local plaintext examples explicitly use `insecure: true`. For gRPC set
`protocol: GRPC` and `endpoint: "host:port"`; the WASM adapter supplies the RPC method.
Connections own endpoint/auth/TLS configuration; module manifests own method
signatures. Credentials stay on the host and use the existing private channel
to compute actors. Module registration and connection creation do not call the
external service.

### Create connections with SQL

Use `--init-query=connections.sql` with `--compute=in-place` and enabled
`WasmServices` to create private FQ connections before submitting queries:

```sql
CREATE EXTERNAL DATA SOURCE profiles_http WITH (
    SOURCE_TYPE = 'ExternalService',
    PROTOCOL = 'HTTP',
    LOCATION = 'https://profiles.example.test',
    AUTH_METHOD = 'TOKEN',
    TOKEN_SECRET_NAME = 'profiles_token'
);
```

For gRPC, use `PROTOCOL='GRPC'` and `LOCATION='host:port'`. For HTTP,
`LOCATION` is an origin, without an application path or query. The WASM module
supplies the HTTP verb/path or gRPC method per call; `METHOD` is not a DDL option.
`USE_TLS` defaults to `TRUE`; plaintext requires `USE_TLS=FALSE` and an `http://`
URL for HTTP. `CA_CERTIFICATE` accepts PEM trust roots, not a filesystem path.
TLS verification cannot be disabled. Unknown properties and duplicate keys fail.

`AUTH_METHOD` accepts `NONE`, `CURRENT_IAM` (the runner's `YQL_TOKEN`), `TOKEN`,
or `TVM` (see the provider-stub section below).
For `TOKEN`, pass `--secrets-file=secrets.json`, a local JSON object:

```json
{"profiles_token": "example-token"}
```

The local runner resolves `TOKEN_SECRET_NAME` while preparing connections and
passes the token through the existing sensitive FQ credential fields. This is
not a persistent secret registry: stored connections contain the resolved token;
changing the file does not rotate an existing connection. SQL and WASM receive
no secret values. Do not put literal tokens in the SQL file.

Initialization accepts only `CREATE EXTERNAL DATA SOURCE`, optionally with
`IF NOT EXISTS`. It uses the SQL v1 parser, including comments and quoted names.
`USE`, pragmas, variable assignments, SELECT, ALTER, DROP and OR REPLACE are not
supported in initialization files. Connection names must be unqualified aliases;
`--scope-id` chooses the FQ scope. One initialization file can contain multiple
statements; repeat `--init-query` for multiple files, executed in argument order.

All initialization SQL is parsed and validated before creating any objects.
Creation then runs sequentially in each query scope, after protobuf connections
and before bindings and queries, once per run rather than per query loop.
Creation is not transactional: objects already created remain if a later API
request fails. Failure stops query submission even with `--continue-after-fail`.
`IF NOT EXISTS` preserves the existing private connection of the current user,
including endpoint and credentials. Plain CREATE rejects a duplicate name,
including a connection supplied by `--connection`.

Initialization can run without a query (omit `-C`); with embedded metadata the
connections last only for that runner invocation. Use `--hold` to keep the runner
available, or an external `--cp-storage-db` for persistent metadata. Module loading
still uses `WasmServices.Modules`; DDL does not register or download WASM.

### TVM authorization (ticket provider stub)

TVM connection metadata and transport integration are implemented. The default
provider is **unavailable**: actual ticket acquisition will be added when the TVM
client/API contract is supplied. Creating a connection succeeds; executing a
query that uses it fails with `TVM ticket provider is not implemented`, before
sending any request to the external service. For local end-to-end testing,
`fqrun --tvm-mock` selects a test provider that returns the fixed fake ticket
`fqrun-mock-tvm-service-ticket`. It does not contact TVM or use the source secret
to mint a real ticket. Without this explicit option, the default provider
remains unavailable.

The mock delays each ticket by 100 ms by default. Use
`--tvm-mock --tvm-mock-delay-ms=2000` to simulate a two-second wait, or set the
delay to `0` for immediate tickets. A one-shot actor timer delivers the completion;
the mock never polls, sleeps or starts a thread per ticket. Other
requests continue through the same transport while a ticket is pending, and the
wait remains subject to the operation's deadline and cancellation.

`DelayedMockTvmAcquisitionDoesNotBlockOtherRequests` tests this using the same
mock provider as fqrun and a controlled timer scheduler. For both HTTP and gRPC
it holds the first ticket pending, completes a second request (with no TVM or an
immediately ready TVM ticket), then fires the timer and completes the first.
Transport tests run the executor on a user pool with a single thread, so an
additional pool thread cannot hide blocking ticket acquisition. Separate tests
exercise shared operation and byte quotas across transports.

```sql
CREATE EXTERNAL DATA SOURCE profiles_http WITH (
    SOURCE_TYPE = 'ExternalService',
    PROTOCOL = 'HTTP',
    LOCATION = 'https://profiles.example.test',
    AUTH_METHOD = 'TVM',
    TVM_SOURCE_ID = '2020001',
    TVM_DESTINATION_ID = '2020002',
    TVM_SECRET_NAME = 'profiles_tvm_secret'
);
```

Pass `--secrets-file=secrets.json` with a JSON object mapping
`profiles_tvm_secret` to the source application's secret. Source and destination
must be nonzero uint32 IDs. TVM cannot be combined with token/IAM credentials.
For protobuf connections use `external_service.tvm_auth { source_id: ...
destination_id: ... secret: "..." }` instead of `external_service.auth`.
The secret is a sensitive credential, omitted from ordinary connection reads and
masked in diagnostic output. The local file is read at startup; changing its
contents requires restarting and recreating the connection.

`ITvmTicketProvider` in `ydb/core/fq/libs/wasm_services/tvm.h` is the integration
point for the real client. It receives only source ID, destination ID, secret and
deadline. Methods/paths are not inputs to TVM authorization and no TVM method
allowlist is applied. The WASM guest supplies each request method/path independently.
The provider must implement nonblocking acquisition, credential isolation,
ticket caching/refresh, and cancellation of individual waiters.
`Request(credentials, deadline, completion)` executes on the user pool and
returns a cancellable waiter. The callback can run inline or on another thread,
reporting `Ready` or `Failed` exactly once; it only publishes the result and wakes
the transport actor. `Request()`, `Cancel()` and waiter destruction must return
promptly, without blocking for a network reply. Cancellation must tolerate a
racing completion. The provider can be injected into `RegisterServiceTransforms`;
all transforms share it.

The host obtains credentials for each network operation and adds
`X-Ya-Service-Ticket` for HTTP or `x-ya-service-ticket` for gRPC. Acquisition counts
towards the operation's deadline and memory/concurrency limits. Cancellation and
shutdown detach pending waiters. Reserved TVM headers cannot be supplied through
connection headers. Tickets and source secrets never enter WASM memory.
Transport/DQ tests use an explicitly injected test provider. fqrun end-to-end
tests also exercise `--tvm-mock`; neither validates a real TVM server or issuance
protocol.

To run the complete local TVM example, build the runner and mock server, then
start the server in one terminal from the repository root:

```bash
./ya make --build relwithdebinfo ydb/tests/tools/fqrun ydb/tests/tools/fqrun/examples/server
ydb/tests/tools/fqrun/examples/server/fqrun-wasm-mock \
  --output-dir /tmp/fqrun-tvm-example --tvm-mock
```

This generates TVM connections, a dummy source secret and the WASM configuration.
The mock checks only that the service-ticket header is nonempty: HTTP requests
without it receive 401, gRPC calls receive UNAUTHENTICATED. It does not check
ticket signatures, source/destination IDs, paths or methods for authorization.

In another terminal, run:

```bash
ydb/tests/tools/fqrun/fqrun -C analytics --compute=in-place --tvm-mock \
  --cfg=/tmp/fqrun-tvm-example/app.conf \
  --init-query=/tmp/fqrun-tvm-example/connections.sql \
  --secrets-file=/tmp/fqrun-tvm-example/secrets.json \
  --query=/tmp/fqrun-tvm-example/profiles_http.sql
```

The query returns IDs 42 and 43, name `Ada`, score 97. Replace the query file with
`profiles_grpc.sql` to test gRPC or `echo.sql` for the Echo module. Existing TVM
DDL and secret files work with the same runner option.

### Reproducible HTTP/gRPC example

From the repository root, build the runner and the local mock service:

```bash
./ya make --build relwithdebinfo ydb/tests/tools/fqrun ydb/tests/tools/fqrun/examples/server
ydb/tests/tools/fqrun/examples/server/fqrun-wasm-mock --output-dir /tmp/fqrun-wasm-example --batch-rows 2
```

The server stays running and writes a complete config, three connections,
`connections.sql`, `secrets.json` (dummy credentials) and SQL examples into the output directory. It uses the checked-in Profile/Echo WASM
test artifacts. Use `--source-root /absolute/path/to/repo` when starting elsewhere.
The mock listens on loopback using available ports; optional `--http-port` and
`--grpc-port` select fixed ports. Generated connections contain a dummy token.

In another terminal, run either Profile transport:

```bash
ydb/tests/tools/fqrun/fqrun -C analytics --compute=in-place \
  --cfg=/tmp/fqrun-wasm-example/app.conf \
  --init-query=/tmp/fqrun-wasm-example/connections.sql \
  --secrets-file=/tmp/fqrun-wasm-example/secrets.json \
  --query=/tmp/fqrun-wasm-example/profiles_http.sql

ydb/tests/tools/fqrun/fqrun -C analytics --compute=in-place \
  --cfg=/tmp/fqrun-wasm-example/app.conf \
  --init-query=/tmp/fqrun-wasm-example/connections.sql \
  --secrets-file=/tmp/fqrun-wasm-example/secrets.json \
  --query=/tmp/fqrun-wasm-example/profiles_grpc.sql
```

Both return IDs 42 and 43, name `Ada` and score 97. For the second module use
`echo.sql` with the same initialization files; it returns `hello` with length 5.
The generated `.pb.txt` connections remain available for the `--connection` interface.

The SQL uses the current prototype interface:

```sql
$input = SELECT 42ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
  WITH CONNECTION='profiles_http', INPUT_TYPE=Struct<id:Uint64>,
       OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>, BATCH_SIZE=8;
SELECT * FROM $profiles;
```

`BATCH_SIZE=1` in the `WITH` clause selects scalar calls; `BATCH_SIZE=64`
requests batches of up to 64 rows. The value must be a constant Int32 in 1..64.
When omitted, `MaxBatchRows` supplies the default (zero also means one), not a
cap on SQL. The method must support batching for values above one; its row and
byte limits and DQ delivery can produce smaller batches. `CallTimeoutMs` bounds an adapter
call. SIGINT/SIGTERM requests FQ query cancellation and waits up to 30 seconds
for terminal status before local runtime shutdown. The default FQ control
heartbeat can take 15 seconds to deliver cancellation. Interrupted runs exit
unsuccessfully.

Use ID 999 with the mock to exercise slow calls/timeouts; ID 400 produces an
invalid HTTP response. Stop the mock with Ctrl-C after finishing the examples.

To build new guest artifacts see `ydb/udfs/wasm/profile/README.md` and
`ydb/udfs/wasm/echo/README.md`; the manifest and module must agree on the service
ABI. Object-style SQL and cache/singleflight remain separate steps of the RFC.

For streaming use `-C stream` (or `-C async-stream`) with `--compute=in-place`.
Add `--checkpoints-db` to enable the existing checkpoint coordinator with the
local database, or `--checkpoints-db=database@endpoint` for an external database.
Local WASM loading is unchanged. A durable recovery setup also needs persistent
control-plane/checkpoint storage and retained query identity; restarting an
ordinary ephemeral fqrun process alone is not a recovery procedure.
The common transform forwards barriers after pending results; external calls may
repeat after DQ recovery and service-side idempotency remains the service's responsibility.

### Verification

```bash
./ya make --build relwithdebinfo -tA ydb/tests/tools/fqrun/tests
```

These tests launch the actual fqrun binary against isolated local HTTP/gRPC mocks,
including TLS, both modules, batching, cancellation, deadlines, invalid inputs and
ordinary analytics with/without explicit backend selection. No installed ydbd or
external service is required.

Transport unit tests additionally cover progress on a single-thread user pool,
delayed TVM tickets, shared operation/memory limits and release of reservations
after cancellation. They also hold HTTP/gRPC responses pending, check that the
executor stops dispatching until readiness arrives, and verify that completion
runs on the user pool:

```bash
./ya make --build relwithdebinfo -tA ydb/core/fq/libs/wasm_services/ut
```

SIGINT and SIGTERM also interrupt `--loop-delay` and `--query-delay` waits.
These waits use a monotonic deadline and check cancellation at most every 50 ms;
shutdown then follows the normal runner cleanup path.

### Module source

fqrun always selects `WasmServices.ModuleSource: LOCAL_FILES`. Supply local
`.wasm`/manifest pairs through `Modules` (or the existing `ModulePath` shorthand);
no UDF Store setup or upload is required. Modules compile at runner startup.
`ydbd` instead defaults to the existing UDF Store and loads the current module
when creating a transform. Both paths share external-call execution, including
HTTP/gRPC, credentials, user-pool scheduling, quotas, deadlines and cancellation. There is
no service-specific version pinning; existing compartments keep their loaded code.

### Service call monitoring

Add `--monitoring --hold` to an existing fqrun service example command. The
printed monitoring port serves `/counters/counters=yq/json` (and `prometheus`
in place of `json`). `--hold` keeps counters accessible after the query finishes;
stop the process with SIGTERM when finished.

Service counters are under `subsystem=wasm_services/component=requests`, labeled
by `protocol` and the guest's per-call `call_alias`. Profile exports
`profile.lookup` / `profile.batch`, Echo `echo.call`. The same registration is
used by ydbd with UDF Store; fqrun continues loading modules from local files.
See the [common metric schema](../../../core/fq/libs/wasm_services/MONITORING.md)
for outcome, latency, queue, payload, reservation and cleanup semantics.
Look at latency histograms by alias, then errors/admission rejections and cleanup
gauges. Metrics aggregate call classes, not individual SQL query IDs.

## Query tracing

Append `--trace-file=/tmp/wasm-trace.jsonl` to a WASM service query invocation.
The runner prints `Query <query_id> trace_id=<trace_id>` and writes completed
Wilson spans as JSON lines with hex trace/span IDs. Follow `parent_span_id` from
`External call` (including `auth.tvm`) through `WASM invocation`, `WASM transform`
and `ComputeActor` to `SQL execution`. Aliases match the metrics, including
`profile.lookup` for scalar and `profile.batch` for batched Profile calls.

Capture is bounded (10000 spans, 16 MiB serialized data) and best-effort on abrupt
shutdown. `MaxTraceSpansPerTransform` controls detail generation; its default is
4096. Local files remain the module source. See
[tracing and sampling](../../../core/fq/libs/wasm_services/MONITORING.md#query-tracing)
for ydbd/UDF Store, Wilson/OTLP configuration and lifecycle semantics.

### Authorization through a WASM module

`AUTH_METHOD='WASM'` selects an explicitly approved auth provider. Its parameters are JSON strings; fields declared as `Secret` in the auth manifest contain references, never secret bytes. The module has a private memory/import realm in the application compartment. The host applies its returned credential header to HTTP/gRPC requests. A compartment-local cache coalesces refreshes; after checkpoint recovery the cache is empty.

Example connection (use `/Root/profiles` and a schema secret path with KQP):

```sql
CREATE EXTERNAL DATA SOURCE profiles WITH (
    SOURCE_TYPE='ExternalService', PROTOCOL='HTTP',
    LOCATION='http://127.0.0.1:8080', USE_TLS=FALSE,
    AUTH_METHOD='WASM', AUTH_PROVIDER='TEST_AUTH',
    AUTH_PARAMETERS='{"secret":"test_secret"}'
);
```

Add the synthetic `ydb/udfs/wasm/auth_test/ut/data/auth_test.wasm` and its `manifest.json` to `WasmServices.Modules`, alongside the application module. The operator config also needs:

```protobuf
AuthProviders {
    Provider: "TEST_AUTH"
    Module: "TestAuth"
    Endpoint: "http://127.0.0.1:8081"
    Alias: "auth.test"
}
```

The synthetic issuer accepts `POST /auth` containing the test secret and responds with the envelope defined in `ydb/udfs/wasm/sdk/services/auth.h`. It is exercised by the runner tests' local HTTP server. It is not a TVM protocol or client.

In fqrun pass `--secrets-file` with `{"test_secret":"local-test-value"}`. WASM auth rereads that file asynchronously at every refresh; replace it atomically to rotate secrets. In kqprun use the existing `CREATE SECRET` and an absolute path in `AUTH_PARAMETERS`, as on ydbd; an explicit `AuthSecretsFile` config is available for trusted local tests. No resolver falls back to a previously read secret. A valid cached ticket remains usable until its TTL.

Kqprun local mode uses repeated `--wasm-service-module` options for application/auth artifacts (each with its adjacent manifest). Store mode uses `--wasm-service-store` and existing UDF upload APIs for both artifacts. Uploading an auth artifact does not approve it: `AuthProviders` is always operator configuration. No real TVM code or artifacts are shipped in these examples.


### Service calls in SELECT and row lambdas

A manifest service method can be used in a row projection. The last argument is
an options struct with a constant connection name and an optional constant
`batch_size` (Int32, 1..64). Omitting it uses the configured default.

```sql
$options = AsStruct('profiles_http' AS connection, 8 AS batch_size);
$lookup = ($id) -> {
    RETURN WASM_PROFILE::Profile($id, $options);
};
$input = SELECT * FROM AS_TABLE(AsList(AsStruct(42ul AS id), AsStruct(43ul AS id)));
$enriched = SELECT id, $lookup(id) AS profile FROM $input;
SELECT id, profile.name AS name, profile.score AS score FROM $enriched;
```

`WASM_PROFILE::Profile(id, $options)` can also appear directly in SELECT. Its
arguments and result fields are defined by `service_methods` in the manifest.
The lambda receives one row; the compiler batches calls across rows using the
existing asynchronous transform. Original rows stay on the host and count
against its byte quota. Duplicate input keys remain separate rows.

IF/CASE calls execute only for selected rows, and branch decisions are retained
across the asynchronous wait. An unselected call performs no guest invocation,
network request or auth-ticket acquisition. Connection resolution and ACL checks
still apply during preparation. Separate calls become separate transforms, each
with its own compartment and auth cache; independent calls are not promised
parallel fan-out.

The synchronous UDF ABI and `PROCESS ... USING EXTERNAL FUNCTION` remain available.
Arbitrary collection lambdas such as ListMap/Fold and service calls under
Coalesce/IfPresent/And/Or are outside this interface; use IF/CASE for conditional
calls. The existing checkpoint/recovery model applies, including possible replay
of external effects after recovery.
