# SDK rediscovery after a database switchover

## Managed EDS cutover

The EDS fixture switches the bootstrap route while the old data nodes stay healthy. It ends only the database's `StreamEndpoints` RPC. A new watch receives a complete assignment from the selected replica. The C++ functional test checks both a generation-stamped close and a fixed gRPC `UNAVAILABLE` close. The latter makes the driver request a current-route check from the balancer.

When the PHP driver processes a switch it cancels outstanding native calls, including suspended iterators. A fresh call uses the destination even when the advertised data address is reused. A synchronous PHP process that is idle must enter the SDK again to process discovery.

Run the native server and C++ tests from the repository root.

```sh
./ya make --build relwithdebinfo -tA ydb/tests/functional/sdk/rediscovery
```

For the seven SDKs maintained outside this repository, `sdk-patches/sources.json` pins each public source revision. The following command creates one checkout per SDK with the source changes staged. Each destination directory must be new.

```sh
cd ydb/tests/functional/sdk/rediscovery
python3 sdk-patches/prepare.py /tmp/ydb-eds-sdks
```

| SDK | Draft opt-in |
| --- | --- |
| C++ | `TDriverConfig::SetDiscoveryMode(EDiscoveryMode::Eds)` |
| Go | `ydb.WithEndpointDiscoveryStream()` |
| Python (sync and asyncio) | `use_eds=True` |
| Java | `GrpcTransportBuilder.withEdsDiscovery()` |
| JavaScript | Driver option `'ydb.sdk.enable_eds': true` |
| .NET | `EnableEdsDiscovery=true` |
| PHP | `'discovery' => true, 'eds' => true` |
| Rust | Connection string parameter `use_discovery=eds` |

Build each probe against its patched checkout. For example, use a local Go module replacement and the probe’s `eds` build tag.

```sh
cp -R clients/go /tmp/ydb-eds-go-probe
(cd /tmp/ydb-eds-go-probe && go mod edit -replace github.com/ydb-platform/ydb-go-sdk/v3=/tmp/ydb-eds-sdks/go && go mod tidy && go build -tags eds -o probe .)
python3 run_eds_probe.py --database /Root/tenant \
  --replica localhost:2136 http://localhost:8765 \
  --replica localhost:3136 http://localhost:9765 -- /tmp/ydb-eds-go-probe/probe
```

The runner requires Python 3.11+, grpcio and two isolated plaintext YDB tenants with the same path and EDS support. It runs the supplied Scheme probe in both directions, compares each server's ListDirectory counters and checks whether the bootstrap TCP peer stays the same. On Linux it verifies that an established source-node socket existed before cutover and that every such socket closes after recovery. It also samples the total number of established sockets to the three test listeners. Concurrent workloads on those tenants affect the counters.

## Transport-disconnect experiment


This test keeps a single driver running while a stable discovery address changes from one YDB database to another. Two advertised addresses initially reach the source. After the first successful operation, the fixture drops the selected TCP transport during the next unary RPC, or disconnects the established Topic stream. The second cached source address stays healthy. Both backends run real YDB services.

A passing driver fetches the destination addresses, completes another operation through the destination within four seconds, and sends no new RPC to a surviving source address. The fixture also checks the peak number of open client connections to its discovery and data listeners. The four-second default is below the C++ driver's five-second periodic check.

## Prepare the databases

Use two **disposable plaintext test databases** with the same database path. Supply a data-node gRPC address for each. For the Scheme tests the databases can be empty.

For BulkUpsert, create a table named `probe` in each database with `id Uint64 NOT NULL` as its primary key. For Topic, create a one-partition topic named `topic` with a consumer named `probe` in each database. Seed each topic with a message whose payload is that backend's literal `host:port` argument. The reader checks both payloads to verify which database delivered the data. Run one reader at a time for this consumer.

## Prepare a probe

Run the build command from the corresponding client directory. Set up a Python 3.11+ virtual environment for the fixture and install `clients/python/requirements.txt`.

| Client directory | Build or install | Supported probes |
| --- | --- | --- |
| cpp | From the repository root run `./ya make --build relwithdebinfo ydb/tests/functional/sdk/rediscovery/clients/cpp` | scheme, bulk-upsert, topic-read, topic-write |
| dotnet | `dotnet build -c Release -p:RestoreLockedMode=true` with .NET SDK 8 | scheme |
| go | `go build -mod=readonly -o rediscovery-probe .` | scheme, topic-write |
| java | `mvn -q package` with JDK 17+ | scheme |
| javascript | `npm install --ignore-scripts` with Node 20.19+ | scheme |
| php | `composer install` with PHP 8 and the grpc, bcmath and curl extensions | scheme |
| python | Use the fixture virtual environment | scheme, topic-read, topic-write |
| rust | `cargo build` with Rust 1.88+ | scheme |

### Build the JavaScript and Rust probes with the EDS patches

Prepare the SDK checkouts using `sdk-patches/prepare.py`. The commands below use
`JS_SDK` and `RUST_SDK` for their absolute paths and run from this fixture directory.

```sh
(cd "$JS_SDK" && npm ci && npm run build -- --filter=@ydbjs/topic)
cp clients/javascript/main.mjs "$JS_SDK/rediscovery-probe.mjs"
cargo build --manifest-path clients/rust/Cargo.toml --config "patch.crates-io.ydb.path='${RUST_SDK}/ydb'"
```

Set `YDB_EDS_DISCOVERY=1` for the patched SDKs. Run
`node "$JS_SDK/rediscovery-probe.mjs"` or `clients/rust/target/debug/ydb-rediscovery-probe`.
Both probes support `YDB_PROBE=scheme`, `topic-read` and `topic-write`.
Set `YDB_TOPIC_PATH`, `YDB_CONSUMER` or `YDB_PRODUCER` to isolate concurrent Topic runs.
The Scheme probe uses released SDKs with the installation commands above when EDS is disabled.

## Run

From this directory, with the virtual environment active, run the following against your two test databases. Each invocation exits nonzero on failure, including for released SDKs that still select a cached source. Set `PYTHON`, `JAVA`, `NODE`, `PHP`, or `DOTNET` to select a specific probe runtime when needed.

```sh
python cutover.py --source localhost:2136 --destination localhost:3136 --database /local --client cpp --probe topic-write

for sdk in cpp dotnet go java javascript php python rust; do
    python cutover.py --source localhost:2136 --destination localhost:3136 --database /local --client "$sdk"
done
```

Swap the two backend arguments to test cutover in the opposite direction. For cross-version coverage, run the backends with the two server versions under test.

### Disconnect through the source nodes

With two distinct tenant nodes serving the source database, the fixture can use the node-admin operation. Supply each source gRPC address and its monitoring URL. Both source nodes must implement `POST /actors/grpc_disconnect` and allow the supplied administrator. Set `YDB_CUTOVER_ADMIN_TOKEN` in the fixture environment. The fixture removes this variable from the SDK probe environment.

```sh
python cutover.py --client cpp --source localhost:2136 --source-peer localhost:2137 \
    --source-admin http://localhost:8765 http://localhost:8766 \
    --destination localhost:3136 --database /local/tenant
```

After the first successful source operation, the fixture publishes the destination and posts to both source nodes. Both source addresses continue accepting connections. The recovery timer and stale-RPC check start when a source relay observes gRPC's HTTP/2 GOAWAY or the server socket closing. The fixture records the first destination success, watches for stale RPCs for another 600 ms and reports peak concurrent SDK connections across discovery and data relays. Run probes sequentially against a shared source because each POST disconnects every client on its node.
