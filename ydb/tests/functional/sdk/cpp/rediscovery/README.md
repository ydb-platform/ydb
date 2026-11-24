# C++ replica switch probe for long-poll discovery

A managed bootstrap route can wake an idle driver after a replica switch. The SDK uses its existing bootstrap address and the ordinary authenticated `Ydb.Discovery.V1.DiscoveryService/ListEndpoints` RPC. Both replicas may continue serving traffic. The source database does not receive a switch notification.

| Request | Managed AWACS worker behavior |
| --- | --- |
| No opt-in metadata | Forward as ordinary discovery. |
| `x-ydb-discovery-long-poll: 1` | Immediately return the selected replica's full endpoint list and `x-ydb-route-generation` response metadata. |
| Opt-in plus `x-ydb-if-route-generation: N` | When the worker is at N, wait for a change or 30 seconds. Forward a fresh lookup using the selected route. A newer worker responds immediately. A worker behind N returns `UNAVAILABLE`. |

The route owner advances the generation on every switch, including failback. A successful response carries one decimal unsigned 64-bit generation in its initial or trailing metadata. The driver maintains one outstanding poll per discovery context. A worker that omits the generation on initial negotiation gets periodic ordinary discovery. Once a driver accepts a managed generation, an unstamped or older response cannot reinstall source endpoints. Rollback of an enrolled route therefore needs a worker that understands the protocol, or a driver restart to renegotiate a legacy route.

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
