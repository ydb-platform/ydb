# ydbd_mock

A Python mock of the public YDB Bridge API. It runs as standalone pile
processes for local experiments, or in process for the Skipper acceptance
tests in [`../acceptance`](../acceptance/README.md). It tests
client-visible behavior; it does not simulate real storage durability or
distconf consensus, and it cannot certify production failover.

## Open bridge_skipper and the mock-control UI together

After building `ydb/apps/bridge_skipper` and
`ydb/apps/bridge_skipper/ydbd_mock` using this checkout's build instructions:

```bash
YDBD_MOCK_IMPL=ya ydb/apps/bridge_skipper/ydbd_mock/scripts/lab.sh --fresh
```

The single foreground command starts two piles with three hosts each and prints
all endpoints. Open **bridge_skipper** at <http://127.0.0.1:8080/> and
**ydbd_mocks_control** at <http://127.0.0.1:21999/> side by side. The control UI
has a simple pile/host table with **Crash** and **Start** buttons and refreshes
every second. Each host button closes or restores that node's gRPC listener;
the pile buttons kill or restart the shared mock process for all its hosts.

Switch bridge_skipper from Observe to Auto in its UI, allow the initial healthy
evidence to settle, then crash `pile2` or several of its hosts. Watch the evidence,
plan and eventual failover in bridge_skipper; confirmation takes several health
rounds. Start the hosts again to restore availability. If skipper disconnected
the pile, use its Rejoin action as well; Start does not change bridge pile state.

Ctrl-C stops the controller, control UI and mock processes started by this lab.
An already running mock cluster in the selected run directory is refused.
Use `YDBD_MOCK_RUN_DIR` for a separate session and `--skipper-port`,
`--control-port`, `--control-base-port`, and `--grpc-base-port` for different
ports. `--piles a,b,c`, `--nodes-per-pile N`, and `--topology FILE` customize
the mock topology. The launcher prints its run directory and skipper log path;
mock logs are under the run directory's `logs/`.

The default `YDBD_MOCK_IMPL=venv` also works after `scripts/setup_venv.sh`.
`BRIDGE_SKIPPER_BINARY` overrides the built C++ executable.

## Start a standalone mock cluster

Build `ydb/apps/bridge_skipper` and `ydb/apps/bridge_skipper/ydbd_mock` using
this checkout's build instructions. From the repository root:

```bash
S=ydb/apps/bridge_skipper/ydbd_mock/scripts
export YDBD_MOCK_IMPL=ya
$S/mock.sh cluster up --fresh
$S/mock.sh cluster status
```

This starts two pile processes, `pile1` (PRIMARY) and `pile2` (SYNCHRONIZED),
with three nodes each. HTTP control ports are 22000 and 22001; gRPC ports are
22100–22102 and 22110–22112. Files live under `~/ydbd_mock_work/run`; set
`YDBD_MOCK_WORK` or `YDBD_MOCK_RUN_DIR` to use another directory.
`--fresh` clears mock replicas and event logs. Every `cluster up` creates a
new cluster ID.

## Run the C++ bridge_skipper

With the mock running, in another terminal with the same environment:

```bash
S=ydb/apps/bridge_skipper/ydbd_mock/scripts
export YDBD_MOCK_IMPL=ya
$S/skipper.sh
```

The launcher writes a JSON config (valid YAML) containing every direct node
endpoint grouped by pile. A fresh state directory starts the built
`bridge_skipper` in Observe and serves its UI/API at <http://127.0.0.1:8080>.
Enable Manual or Auto through that UI/API; mode is persisted, so restarts restore
it. The launcher has no `--mode` configuration override. Use
`--listen 127.0.0.1:18080` to change the HTTP address.
`BRIDGE_SKIPPER_BINARY` overrides the executable path.

The config and controller state live in `run/bridge_skipper/<cluster_id>/`.
Restarting the controller against the same mock session preserves its state;
a new mock session uses a new directory. `--state-dir DIR` overrides this.
The launcher supports plaintext mock topologies; TLS topologies require a
custom controller config with the appropriate credentials.

Exercise the cluster from another terminal:

```bash
$S/mock.sh cluster kill pile2
$S/mock.sh cluster revive pile2
$S/mock.sh cluster state
$S/mock.sh cluster events --limit 30
```

Stop the controller with Ctrl-C, then stop the mock with
`$S/mock.sh cluster down`. The mock supports any number of piles via
`cluster up --piles a,b,c`; the C++ launcher uses the full inventory.

## Run bridge_skipper_demo

The Python demo also needs a virtualenv and a real YDB CLI. Set up the
virtualenv once, then run it against the same standalone cluster:

```bash
$S/setup_venv.sh
export YDBD_MOCK_REAL_YDB=/path/to/ydb
$S/demo.sh --fresh --tui
```

`demo.sh` uses the guarded CLI shim to map mock node labels to loopback ports.
It defaults to `mock-pile1-1`; use `-e LABEL` for a custom topology. The demo
supports two piles. Its state file is separate from the C++ controller's.
Run one controller at a time when enabling automatic mutations.

To run the mock itself from the virtualenv instead of the built binary, use
`YDBD_MOCK_IMPL=venv` after setup. Regenerate protos with
`$S/gen_protos.sh` after public API changes.

## In-process tests

`lib/testkit.LiveCluster` starts one simulated pile per name, each with several
nodes on their own loopback gRPC ports plus an HTTP control and peer API, all in
the test's event loop:

```python
cluster = LiveCluster(pile_names=("a", "b"), timings=tiny_timings(lifecycle_delay=0.05))
await cluster.start()
endpoints = {pile.name: ["grpc://" + node.endpoint for node in pile.nodes] for pile in cluster.topo.piles}
await cluster.kill("a")
```

The nodes serve `GetClusterState` (plain and with pile connectivity),
`UpdateClusterState` with `expected_generation`, `SelfCheck` with
`BRIDGE_GROUP` RED issues for blamed piles (and the BSC-unavailable fallback),
plus the lifecycle RPCs the mock needs internally. Piles commit snapshots with
a simplified protocol, apply YDB's follow-ups (promotion completion,
stage-2 synchronization, suspension to disconnection) and keep a per-node
request log. The HTTP control API injects faults (down, silence, partitions,
failpoints that hold or fail a method at a stage, replica overrides, timing
and policy changes).

Layout: `lib/` is the implementation (`cluster`, `peers`, `replica`,
`validator`, `followups`, `health`, `services`, `control_api`, `testkit` and
their helpers); `tests/` holds its unit and contract tests, including
`test_contracts`, `test_connectivity_report`, `test_health_modes`,
`test_suspend_quorum` and `test_proto_contract`.

Test target: `ydb/apps/bridge_skipper/ydbd_mock/tests`. The mock emits the
server's texts for bridge group parts and BLUE `Replication in progress` VDISK
reasons for stage-2 piles, `PILE_*` issues for SYNCHRONIZED piles with unavailable nodes
and `HEALTHCHECK_STATUS` on request (control op `healthcheck_status`). Its
reason chains are shallower than the server's, and recorded real-YDB fixtures
remain the release gate for message texts.
