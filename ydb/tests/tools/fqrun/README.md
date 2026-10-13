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

### Create connections with SQL

Use `--init-query=connections.sql` with `--compute=in-place` and enabled
`WasmServices` to create private FQ connections before submitting queries:

```sql
CREATE EXTERNAL DATA SOURCE profiles_http WITH (
    SOURCE_TYPE = 'ExternalService',
    PROTOCOL = 'HTTP',
    LOCATION = 'https://profiles.example.test/profile',
    METHOD = 'POST',
    AUTH_METHOD = 'TOKEN',
    TOKEN_SECRET_NAME = 'profiles_token'
);
```

For gRPC, use `PROTOCOL='GRPC'`, `LOCATION='host:port'` and a fully qualified
`METHOD='/package.Service/Method'`. For HTTP, `METHOD` defaults to `POST`.
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
sending any request to the external service. No synthetic tickets or fallback
unauthenticated calls are made.

```sql
CREATE EXTERNAL DATA SOURCE profiles_http WITH (
    SOURCE_TYPE = 'ExternalService',
    PROTOCOL = 'HTTP',
    LOCATION = 'https://profiles.example.test/profile',
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
allowlist is applied. Existing transport method settings remain independent.
The provider must implement nonblocking acquisition, credential isolation,
ticket caching/refresh, and cancellation of individual waiters. It can be injected
into `RegisterServiceTransforms`; all transforms share that provider.

The host obtains credentials for each network operation and adds
`X-Ya-Service-Ticket` for HTTP or `x-ya-service-ticket` for gRPC. Acquisition counts
towards the operation's deadline and memory/concurrency limits. Cancellation and
shutdown detach pending waiters. Reserved TVM headers cannot be supplied through
connection headers. Tickets and source secrets never enter WASM memory.
Transport/DQ tests use an explicitly injected test provider; these are not a
validation of a real TVM server or issuance protocol.

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
