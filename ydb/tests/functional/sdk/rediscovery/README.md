# Replica rediscovery using unary long polling

A managed bootstrap route can wake an idle driver after a replica switch. The SDK uses its existing bootstrap address and the ordinary authenticated `Ydb.Discovery.V1.DiscoveryService/ListEndpoints` RPC. Both replicas may continue serving traffic. The source database does not receive a switch notification.

| Request | Managed AWACS worker behavior |
| --- | --- |
| No opt-in metadata | Forward as ordinary discovery. |
| `x-ydb-discovery-long-poll: 1` | Immediately return the selected replica's full endpoint list and `x-ydb-route-generation` response metadata. |
| Opt-in plus `x-ydb-if-route-generation: N` | When the worker is at N, wait for a change or 30 seconds. Forward a fresh lookup using the selected route. A newer worker responds immediately. A worker behind N returns `UNAVAILABLE`. |

The route owner advances the generation on every switch, including failback. A successful response carries one decimal unsigned 64-bit generation in its initial or trailing metadata. The driver maintains one outstanding poll per discovery context. A worker that omits the generation on initial negotiation gets periodic ordinary discovery. Once a driver accepts a managed generation, an unstamped or older response cannot reinstall source endpoints. Rollback of an enrolled route therefore needs a worker that understands the protocol, or a driver restart to renegotiate a legacy route.

After the SDK installs a new assignment, fresh RPCs use the destination. An RPC already running on the healthy source can continue until it finishes or its owner closes it. This includes bidirectional Topic and Coordination streams. A node-bound handle that needs a new RPC may have to be recreated. The bootstrap connection is reused. Both data connections can coexist while source work drains.

## SDK drafts

| SDK | Opt-in |
| --- | --- |
| C++ | `TDriverConfig::SetDiscoveryMode(EDiscoveryMode::LongPoll)` |
| Go | `ydb.WithDiscoveryLongPoll()` |
| Python | `use_long_poll=True` |
| Java | `GrpcTransportBuilder.withLongPollDiscovery()` |
| JavaScript | `'ydb.sdk.enable_long_poll': true` |
| .NET | `EnableLongPollDiscovery=true` |
| PHP | `'discovery' => true, 'discoveryLongPoll' => true` |
| Rust | `?use_discovery=long_poll` |

A PHP driver with a running ReactPHP event loop can process discovery while application code is idle. A synchronous process observes discovery on its next SDK call or stream iteration. A blocked native gRPC receive pauses the event loop. PHP applications with a custom in-memory gRPC trust bundle must also supply its CA file for the discovery poll.

The seven external SDK patches target the public revisions in `sdk-patches/sources.json`. Apply them into new local checkouts with `python3 sdk-patches/prepare.py /tmp/ydb-long-poll-sdks`.

## Tests

From the YDB root run `./ya make --build relwithdebinfo -tA ydb/tests/functional/sdk/rediscovery`. The fixture launches two independent databases and a managed bootstrap test double. It checks Scheme, BulkUpsert, existing Topic reader and writer, and held Query, Table, and Coordination sessions. The Coordination test pings the original stream after the same driver reads the destination marker.

To run an external SDK probe, build the corresponding program under `clients/` against its patched checkout. For Go, select the `longpoll` build tag. Run the following from this directory with two isolated plaintext tenants and their monitoring URLs.

```sh
python3 run_long_poll_probe.py --database /Root/tenant \
  --replica localhost:2136 http://localhost:8765 \
  --replica localhost:3136 http://localhost:9765 -- /path/to/probe
```

The runner cuts over in both directions and checks source and destination Scheme request counters, the bootstrap TCP peer, and old-node sockets on Linux. Socket counts are reported so that a draining source transport remains visible. Use monitoring endpoints and data-node ports that belong to the supplied tenants.
