# SDK rediscovery after a database switchover

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
