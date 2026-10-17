# KqpRun tool

Tool can be used to execute queries by using kikimr provider.

For profiling memory allocations build kqprun with ya make flag `-D PROFILE_MEMORY_ALLOCATIONS -D CXXFLAGS=-DPROFILE_MEMORY_ALLOCATIONS`.

## Scripts

* `flame_graph.sh` - script for collecting flame graphs in svg format, usage:
    ```(bash)
    ./scripts/flame_graph.sh [graph collection time in seconds] [use sudo]
    ```

* `start_prometheus.sh` - start prometheus web UI, can be used for counters visualisation (kqprun should be run with flag `-M <monitoring port>`), usage:
    ```(bash)
    ./scripts/start_prometheus.sh <kqprun monitoring port> <web UI port> [config path]
    ```
    Prometheus UI available on `http://localhost:<web UI port>/targets`

* `start_grafana.sh` - start grafana other existing prometheus, usage:
    ```(bash)
    ./scripts/start_grafana.sh <prometheus port> <web UI port> [additional dashboards dirs]
    ```

    Command for starting prometheus and grafana:
    ```(bash)
    ./scripts/start_prometheus.sh 32000 32001 && ./scripts/start_grafana.sh 32001 32002
    ```
    Where 32000 - kqprun monitoring port, graphana UI available on http://localhost:32002 (login: `admin`, password: `admin`)

* `start_connector.sh` - start local FQ connector, usage:
    ```(bash)
    ./scripts/start_connector.sh <connector port>
    ```

* `cleanup_docker.sh` - stop created docker containers, usage:
    ```(bash)
    ./scripts/cleanup_docker.sh [name filter]
    ```

## Examples

### Queries

* Run select 42:
    ```(bash)
    ./kqprun --sql "SELECT 42"
    ```

* Queries shooting:
    ```(bash)
    ./kqprun --sql "SELECT 42" -C async --loop-count 0 --loop-delay 100 --inflight-limit 10
    ```

### Logs

* Setup log settings (`-C query` for clear logs):
    ```(bash)
    ./kqprun --sql "SELECT 42" -C query --log-default=warn --log KQP_YQL=trace --log-file query.log
    ```

* Trace opt:
    ```(bash)
    ./kqprun --sql "SELECT 42" -C query -T script
    ```

* Runtime statistics:
    ```(bash)
    ./kqprun --sql "SELECT 42" --script-statistics stats.log --script-timeline-file timeline.svg
    ```

### Cluster

* Embedded UI, flag `-M` means the beginning of the range of ports used for different nodes:
    ```(bash)
    ./kqprun -M 32000
    ```

    Monitoring endpoint: http://localhost:32000

* gRPC endpoint, flag `-G` means the beginning of the range of ports used for different tenants:
    ```(bash)
    ./kqprun -G 32000
    ```

    Connect with ydb CLI: `ydb -e grpc://localhost:32000 -d /Root`

* Static storage:
    ```(bash)
    ./kqprun -M 32000 --storage-path ./storage --storage-size 32
    ```

* Create serverless domain and execute query in this domain:
    ```(bash)
    ./kqprun -M 32000 --shared my-shared --serverless my-serverless --sql "SELECT 42" -D /Root/my-serverless
    ```

### WASM service adapters

Use the same WASM modules, HTTP/unary gRPC transports, call aliases, metrics,
tracing and user-pool executor as fqrun. Local files do not require UDF Store.
Each `--wasm-service-module` must have an adjacent `<file>.manifest.json`:

```bash
cp ydb/udfs/wasm/profile/ut/data/profile.wasm /tmp/profile.wasm
cp ydb/udfs/wasm/profile/manifest.json /tmp/profile.wasm.manifest.json
./kqprun --wasm-service-module /tmp/profile.wasm \
  -s /tmp/connections.sql -C query -p /tmp/profile.sql
```

For an HTTP service on port 8080, `/tmp/connections.sql` can contain:

```sql
CREATE SECRET `/Root/profile_token` WITH (value='example-token');
CREATE EXTERNAL DATA SOURCE `/Root/profiles` WITH (
    SOURCE_TYPE='ExternalService', PROTOCOL='HTTP',
    LOCATION='http://127.0.0.1:8080', USE_TLS=FALSE,
    AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='/Root/profile_token'
);
```

`/tmp/profile.sql`:

```sql
$input = SELECT 42ul AS id;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
    WITH CONNECTION='/Root/profiles', INPUT_TYPE=Struct<id:Uint64>,
         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT * FROM $profiles;
```

`CONNECTION` is an absolute database path. HTTP locations are origins; paths and
methods come from the WASM code. For gRPC use `PROTOCOL='GRPC'` and a `host:port`
location. TLS is enabled by default; `CA_CERTIFICATE` can provide a PEM root.
The caller needs `SelectRow` on the connection and the normal permissions on
its referenced secret. Secrets are resolved by the existing YDB secret mechanism
and sent to workers in `SecureParams`, not in public invocation settings.

Repeat `--wasm-service-module` for multiple modules, or configure
`FederatedQueryConfig.WasmServices` with `Enabled: true`, `ModuleSource: LOCAL_FILES`
and `Modules { ModulePath: ... ManifestPath: ... }`. Do not mix CLI module paths
with config module paths. This block also controls batch size, deadlines, memory,
concurrency and tracing limits for KQP, independently of whether FQ is enabled.

For the existing UDF Store, start `./kqprun --wasm-service-store --dedicated services -D /Root/services -G`, upload
and compile the module through the UDF service at the printed tenant gRPC port, database
`/Root/services`, then execute the DDL and SQL through Query Service with connection and secret paths under `/Root/services`.
The existing compile controller requires a tenant; Store mode is unavailable in the root domain.
If `KvStorageMedia` is not configured, the runner selects the tenant's storage pool. Store mode
rejects local paths. Modules are loaded from the current compiled Store catalog;
there is no version pinning. An already created compartment keeps its loaded code.

TVM connections use `AUTH_METHOD='TVM'`, `TVM_SOURCE_ID`, `TVM_DESTINATION_ID` and
`TVM_SECRET_PATH`. For local testing pass `--mock-tvm --mock-tvm-delay-ms=100`.
The mock returns fake tickets asynchronously. The real TVM provider remains a
stub and reports an error before calling the external service.

External calls are not participants in the SQL transaction: a rollback or retry
cannot undo an external effect. Service idempotency remains the service's concern.
FQ and KQP share the execution code but have separate registered executors and
budgets. This integration preserves fqrun and its streaming/checkpoint path.


UDF Store mode requires exactly one dedicated/shared tenant in this embedded
process, because the loaded-module catalog and UDF registry are process-wide.
Multiple Store tenants and serverless-only configurations are rejected before
server initialization, including when Store is enabled through app config.
This limit does not apply to local-module mode. Persistent tenants are restored
before validation: reopening `--storage-path` with `--wasm-service-store` does
not require repeating `--dedicated`/`--shared`. Module revisions remain unpinned.
