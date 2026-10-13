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
    endpoint: "https://profiles.example.test/profile"
    method: "POST"
    auth { token { token: "${YQL_TOKEN}" } }
  }
}
```

For that placeholder use `--templates` and provide the existing `YQL_TOKEN`
environment variable. TLS is enabled by default; `ca_certificate` accepts PEM
roots. Local plaintext examples explicitly use `insecure: true`. For gRPC set
`protocol: GRPC`, `endpoint: "host:port"` and the fully qualified RPC `method`.
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
ABI. Object-style SQL, cache/singleflight, digest pinning and streaming recovery
remain separate steps of the service-adapter RFC.

### Verification

```bash
./ya make --build relwithdebinfo -tA ydb/tests/tools/fqrun/tests
```

These tests launch the actual fqrun binary against isolated local HTTP/gRPC mocks,
including TLS, both modules, batching, cancellation, deadlines, invalid inputs and
ordinary analytics with/without explicit backend selection. No installed ydbd or
external service is required.
