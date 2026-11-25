# C++ replica switch probe for long-poll discovery

The C++ driver keeps one conditional unary `ListEndpoints` request on its bootstrap connection. A direct YDB server wakes that request when its database's advertised endpoints change. A managed AWACS route can also wake it when the active replica changes. Both use the existing gRPC port.

| Metadata | Owner | Meaning |
| --- | --- | --- |
| `x-ydb-discovery-long-poll: 1` | SDK | Opts into conditional discovery. Ordinary clients receive the usual immediate reply. |
| `x-ydb-endpoint-version` | YDB | Opaque version of the advertised endpoints and pile states. The SDK echoes it as `x-ydb-if-endpoint-version`. |
| `x-ydb-route-generation` | Managed AWACS worker | Route generation, advanced on every replica switch including failback. The SDK echoes it as `x-ydb-if-route-generation`. |

The initial request returns a full assignment immediately. Direct YDB holds a matching endpoint version for up to 30 seconds, then returns a fresh full assignment. A board change wakes it early. A node joining or leaving only updates the affected endpoints. YDB computes the endpoint version from its filtered snapshot and keeps no durable generation counter.

A managed worker owns the route condition. When the generation matches and the client supplies an endpoint version, the worker forwards the endpoint condition to YDB and watches its own route in parallel. A route change interrupts the old lookup. The driver retries on the same bootstrap connection and receives the new replica's full assignment. A route-only client gets the worker's existing conditional wait. An older worker that forwards the route condition after waiting gets an immediate answer from YDB, which avoids two successive waits.

If neither side advertises support on initial negotiation, the driver refreshes about once per minute. After accepting a managed generation, it rejects missing or older generations. A rollback to a legacy worker on an enrolled route needs a driver restart. Direct YDB upgrades and downgrades negotiate support using the endpoint version alone.

After the SDK installs a new assignment, fresh RPCs use the destination. An RPC already running on the healthy source can continue until it finishes or its owner closes it. This includes bidirectional Topic and Coordination streams. A node-bound handle that needs a new RPC may have to be recreated. The bootstrap connection is reused. Both data connections can coexist while source work drains.

Enable this mode in a C++ application with `TDriverConfig::SetDiscoveryMode(EDiscoveryMode::LongPoll)`. To build and run the probe from the repository root, point it at a managed bootstrap endpoint serving the test database.

```sh
./ya make --build relwithdebinfo ydb/tests/functional/sdk/cpp/rediscovery
YDB_ENDPOINT=localhost:2136 YDB_DATABASE=/Root/tenant YDB_LONG_POLL_DISCOVERY=1 \
  YDB_PROBE=list-on-demand ydb/tests/functional/sdk/cpp/rediscovery/rediscovery-probe
```

Enter `list` before and after switching the bootstrap route. With a distinct directory on each replica, the returned names show which replica handled each fresh request on the same driver.

The native regression tests use gRPC servers to exercise discovery and requests held across a switch.

```sh
./ya make --build relwithdebinfo -DUSER_CXXFLAGS=-Werror -tA ydb/public/sdk/cpp/tests/unit/client/driver -F 'EndpointRediscovery::*'
```

To check the actual YDB server, the following test publishes an endpoint while a direct gRPC poll is outstanding and checks the returned version and membership.

```sh
./ya make --build relwithdebinfo -tA ydb/services/ydb/ut -F 'YdbDiscoveryLongPoll::DirectPollWakesOnBoardChange'
```
