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

The experimental service adapters run in analytical FQ/DQ in-place execution.
Use `-C analytics --compute=in-place`; `async-analytics` can submit concurrent
queries. This is separate from native UDF loading with `--udf`/`--udfs-dir`.
Streaming/checkpoints and YDB/KQP compute are unsupported for this integration.

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
processes responses on the user pool. gRPC completion callbacks only publish
their results and wake the actor. WASM resumes through its owning compute
actor when data is ready. No transform creates its own transport thread. While
requests are waiting, the user-pool thread is available to other work.

Each turn processes at most 32 ready transport sessions before yielding to the
actor scheduler. There is no periodic scan of pending requests. One-shot timers
cover concrete request deadlines, libcurl timeouts and native timer operations. Request preparation, transport
progress, response processing and owner notifications share the user pool's CPU
budget.

`MaxTransportOperations` and `MaxTransportBytes` in the configuration above are
shared limits for all WASM service transports registered on the FQ node. Their
defaults are 1024 operations and 256 MiB. Each individual transport also retains
its limits of 128 operations and 64 MiB. Admission must fit both sets of limits;
otherwise the operation fails immediately with `ResourceLimit`, without adding
it to an unbounded waiting queue. Operations waiting for a timer and operations whose cancellation is still
being cleaned up consume credits.

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

### Reproducible HTTP/gRPC example

From the repository root, build the runner and the local mock service:

```bash
./ya make --build relwithdebinfo ydb/tests/tools/fqrun ydb/tests/tools/fqrun/examples/server
ydb/tests/tools/fqrun/examples/server/fqrun-wasm-mock --output-dir /tmp/fqrun-wasm-example --batch-rows 2
```

The server stays running and writes a complete config, three connections and SQL
examples into the output directory. It uses the checked-in Profile/Echo WASM
test artifacts. Use `--source-root /absolute/path/to/repo` when starting elsewhere.
The mock listens on loopback using available ports; optional `--http-port` and
`--grpc-port` select fixed ports. Generated connections contain a dummy token.

In another terminal, run either Profile transport:

```bash
ydb/tests/tools/fqrun/fqrun -C analytics --compute=in-place \
  --cfg=/tmp/fqrun-wasm-example/app.conf \
  --connection=/tmp/fqrun-wasm-example/profiles_http.pb.txt \
  --query=/tmp/fqrun-wasm-example/profiles_http.sql

ydb/tests/tools/fqrun/fqrun -C analytics --compute=in-place \
  --cfg=/tmp/fqrun-wasm-example/app.conf \
  --connection=/tmp/fqrun-wasm-example/profiles_grpc.pb.txt \
  --query=/tmp/fqrun-wasm-example/profiles_grpc.sql
```

Both return IDs 42 and 43, name `Ada` and score 97. For the second module use
`echo_http.pb.txt` and `echo.sql`; it returns `hello` with length 5.
Multiple `--connection` arguments can be supplied to a single run.

The SQL uses the current prototype interface:

```sql
$input = SELECT 42ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
  WITH CONNECTION='profiles_http', INPUT_TYPE=Struct<id:Uint64>,
       OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT * FROM $profiles;
```

`MaxBatchRows: 1` selects scalar calls. Larger values request batching supported
by the method; DQ can deliver smaller batches. `CallTimeoutMs` bounds an adapter
call. SIGINT/SIGTERM requests FQ query cancellation and waits up to 30 seconds
for terminal status before local runtime shutdown. The default FQ control
heartbeat can take 15 seconds to deliver cancellation. Interrupted runs exit
unsuccessfully.

Use ID 999 with the mock to exercise slow calls/timeouts; ID 400 produces an
invalid HTTP response. Stop the mock with Ctrl-C after finishing the examples.

To build new guest artifacts see `ydb/udfs/wasm/profile/README.md` and
`ydb/udfs/wasm/echo/README.md`; the manifest and module must agree on the service
ABI. Object-style SQL, cache/singleflight and streaming recovery
remain separate steps of the service-adapter RFC.

### Verification

```bash
./ya make --build relwithdebinfo -tA ydb/tests/tools/fqrun/tests
```

These tests launch the actual fqrun binary against isolated local HTTP/gRPC mocks,
including TLS, both modules, batching, cancellation, deadlines, invalid inputs and
ordinary analytics with/without explicit backend selection. No installed ydbd or
external service is required.

Transport unit tests additionally cover progress on a single-thread user pool,
shared operation/memory limits and release of reservations after cancellation. They also hold HTTP/gRPC responses pending, check that the
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
