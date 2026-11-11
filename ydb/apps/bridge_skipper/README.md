# YDB Bridge Skipper

Skipper is one external failover controller for one YDB bridge cluster. It
observes `GetClusterState`, pile connectivity and whole-cluster `SelfCheck`
through the public C++ SDK. When failed piles are confirmed, it plans one
guarded update that disconnects them all and, if needed, promotes a survivor.
A fresh state directory starts in Observe: observation continues with no dispatch.
The embedded web UI at `/` enables Manual reviewed failover, switchover, take
offline and rejoin, or Auto policy-approved failover. Manual and Auto use the
same guarded failover planner. Mode and typed pending work are persisted;
configuration has no `mode` key. Membership and conversion remain external CLI
operations.

* [Operator runbook](docs/operations.md): deployment, configuration, reviewed
  commands, Auto, lifecycle operations, pending mutations and recovery.
* [Monitoring](docs/monitoring.md) and [example alerts](docs/alerts.yaml):
  `/api/state`, metrics, logs and alerts.
* [Configuration example](config/example.yaml).
* [Implementation guide](IMPLEMENTATION-index.md): layout, threading model,
  invariants and tests.
* [Mock](ydbd_mock/README.md) and [end-to-end suite](acceptance/README.md).

## Safety summary

- Run exactly one active controller, in a third failure domain that reaches every pile. The state-directory lock does not exclude another host, another state directory or an administrator; there is no server lease or fencing token.
- Each configured endpoint addresses exactly one YDB node (no balancer) and is keyed by the pile its SelfCheck reports. Independently verify that every configured node belongs to the intended cluster: a matching pile name and the configured local `cluster_id` do not verify remote cluster identity.
- Skipper acts only when every Down (Failed or Lost) pile is confirmed at one generation over evidence sent at least `confirm_seconds` apart, and disconnects them all in one update. `confirm_seconds` is measured over the send times of the supporting evidence, not from the first Down observation (`down_since`), so the mutation can follow `down_since` by less than `confirm_seconds` when RED evidence sent before that observation already spans part of the window.
- Healthy requires clean BSC replies whose send times span `bsc_warmup_seconds` without a fresh unclean reply, a fresh fallback reply from the BSC-host pile, a role change, or a gap of more than `evidence_ttl_seconds` between consecutive clean replies; after a role change only sends at or after its observation can establish the new span. The span restarts at the first BSC reply after an endpoint returns from fallback to BSC mode. A round whose SelfCheck is still in flight is sampled as soon as the call settles. Fallback and GREY no-data never prove health; replication alone never triggers failover.
- Failed normally needs every fresh BSC-mode reply to report a RED group part for the pile. With no fresh BSC reply anywhere, the BSC-host pile (PROMOTED, otherwise PRIMARY) can also become Failed when every current outside fallback reply reports an attributed RED static group with complete VDisk reasons including actual RED, no reply is truncated. An own node-quorum claim blocks this generic fallback rule. A separate storage-failure rule can override that veto only if every current outside fallback reply proves a RED static group whose complete, uniquely referenced reasons are all RED VDisks caused by an explicitly reported same-node, same-pile PDisk `DeviceIoError`. Replication, transport failures, missing or ambiguous reasons do not qualify. Missing, GREY and replication-only parts do not suffice. Only fallback sends after the observed generation and, after a role change, its `bsc_warmup_seconds` boundary count; confirmation restarts on a generation or failure-source change. Lost needs fresh settled connectivity reports (sent while the reporter's root was not binding) from other piles at the current generation that all show the pile in config quorum without node quorum, and no report from the pile itself that shows its node quorum; a report sent while binding counts only for positive claims (the reporter's own node quorum, participation and survivor visibility). Binding alone never proves Lost. A primary loss with only GREY or missing fallback parts still needs manual action from the published state; decisive fallback RED can establish Failed while binding persists.
- A Down primary is replaced only by an eligible survivor: Healthy, or, with no fresh BSC-mode reply anywhere, self quorum, its own fresh non-truncated fallback reply, and no fresh fallback-mode reply attributing a RED or GREY group part to it (the PROMOTED pile first, else the first eligible SYNCHRONIZED pile by name). A primary that is not Down stays primary whatever its verdict. There is no additional candidate confirmation window. Neither node quorum nor a clean fallback reply proves full disk health; the no-BSC rule still needs measurement M3 below.
- Endpoints are static configuration keyed by pile, a documented deviation from the design overview's FetchConfig/ListEndpoints inventory: a configured pile absent from the selected view is ignored with a `configured_endpoint_ignored` warning, and a membership change needs a configuration change and a restart.
- Zero data loss remains the product invariant. Reviews describe quorum participation and application availability; uncertain outcomes do not change that invariant.
- Every update carries `expected_generation`. A persisted high-water mark and pending record ensure that only the identical update, or a plan that keeps its primary and adds DISCONNECTED targets, is ever sent at a generation.
- Client certificates are the recommended production credentials. A token file is re-read before every update and after any UNAUTHORIZED reply, so rotation normally needs no restart. A rejected mutation stops Auto at that generation even after the token changes; fix the cause and restart, or wait for a newer generation, as described in the [runbook](docs/operations.md#pending-mutations-and-state-overrides).
- A generation guard cannot choose between divergent histories after a partition.
- Known policy limits are listed in the [implementation guide](IMPLEMENTATION-index.md#observation-and-policy-invariants).

## Failover timing

The defaults target a committed failover within 20 seconds of a provable failure
in a previously healthy cluster with a surviving commit quorum. State and health
rounds run every second, GetClusterState has a 1-second deadline, SelfCheck has a
4-second operation timeout (6-second client deadline), evidence expires after
8 seconds, and supporting failure evidence must span 2 seconds. BSC receives
half the SelfCheck operation timeout. Fresh BSC failures can be confirmed in a
few seconds; a lost primary can first require cached positive claims to expire.
A qualifying fresh fallback failure takes precedence over retained settled
connectivity loss. Otherwise a cached loss sample can stall confirmation until
its TTL expires, despite advancing failure evidence from SelfCheck.

`confirm_seconds` is a minimum evidence span, not a total failover deadline.
Increasing TTL, sampling intervals or confirmation extends failover latency.
Existing deployments that explicitly configure the old values retain them and
must update their configuration to use the new budget. Partial overrides must
still satisfy the freshness constraints: explicitly increase `evidence_ttl_seconds`
when retaining longer RPC timeouts or sampling intervals. More frequent SelfCheck
requests and its shorter BSC budget require qualification under deployment load.
The 30-second BSC warm-up is unchanged: startup and recent role changes may take
longer. GREY or missing evidence, history conflicts, no eligible survivor and
unavailable commit quorums still block automation; the time budget never forces
an unsafe mutation. This is not a bound on a stalled server's response time.

## Required builds and release checklist

No released numeric minimum version is established here. Pin actual server,
SDK, CLI and Skipper build IDs. None of the commits below is on `main` yet.
Every receiving/forwarding server node used by the controller must include
these server changes (or verified equivalents):

| Required behavior | Source commit |
| --- | --- |
| `expected_generation` checked by the server | `ab94a1660dd` |
| Pile connectivity report | `ad5c63f8215` |
| Fallback missing whiteboard data remains GREY unknown | `47c54ff9176` |
| Stage-2 SelfCheck group-part visibility | `6d0f8ee7a75` |

Older server nodes can ignore `expected_generation`. Skipper does not probe for
support. Separately, the SDK and CLI must include generation-guard support
(`bab4be64648`), and the SDK used to build Skipper must include the
pile-connectivity extension in
`ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_bridge.h` and
`ydb/public/sdk/cpp/src/client/draft/ydb_bridge.cpp`. These are client
requirements, not server-node commits. The named CLI lifecycle commands have not gained the
optional generation flag; the runbook uses guarded hidden `update` for overlays.

**Production Auto release validation remains NOT DONE.** Unit and mock passes
cannot establish real storage behavior, supported-build conformance or loaded
cluster timing. Before enabling production Auto for a supported build, complete
and retain:

- [ ] Actual server build/commit, CLI build, Skipper build, topology, configuration
  and recording date: **NOT RECORDED**.
- [ ] Record real SelfCheck replies and connectivity reports from a bridge
  cluster with at least two piles, using the request Skipper sends
  (`return_verbose_status = false`, `merge_records = false`,
  `do_not_cache = true`, no `minimum_status`/`maximum_level`, 4 s operation
  timeout) from a node of a surviving pile, through the CLI's raw healthcheck
  output or a short SDK program printing `SelfCheckResult` in protobuf text
  format. Store each as `domain/ut/fixtures/real_<case>.pbtxt`, add the server
  commit, cluster shape and case to `domain/ut/fixtures/README.md`, and pin
  the classifier with one `health_ut` test per file (`ClassifySelfCheck`,
  `EvaluatePiles`, `Decide`). Cases: (1) BSC mode, clean; (2) BSC mode with a
  RED `STORAGE_GROUP`/`BRIDGE_GROUP` part for a pile whose nodes were killed;
  (3) a silent (black-holed, no RST) loss of the primary and of a
  non-primary; (4) fallback mode after black-holing the BSC-host pile,
  expecting the RED `System tablet BSC didn't provide information` marker and
  GREY `Group has disks with no data` parts; (5) PROMOTED loss during a held
  or incomplete switchover; (6) an A|B partition with BSC on each side; (7) a
  dead stage-2 pile; (8) BLUE `Replication in progress` while a rejoined pile
  resynchronizes; (9) `PILE_STATE_STORAGE`/`PILE_SCHEME_BOARD`/`PILE_BOARD`
  issues after stopping one pile's ring nodes; (10) a `HEALTHCHECK_STATUS`
  YELLOW/ORANGE/RED issue and a truncated reply, if a cluster large enough is
  available, otherwise mark the case synthetic.
- [ ] M1, binding duty cycle: run Skipper in `manual` mode with endpoints in
  every pile. Black-hole every host of one non-primary pile (drop all IP
  traffic, no RST). Sample `GET /api/state` once per second for 5 minutes and
  read `piles[].connectivity[].binding_in_progress`. Per surviving vantage
  pile record the fraction of samples whose newest report is binding and the
  longest run of consecutive binding samples in seconds. Repeat with the
  primary pile black-holed. Record both in the release evidence; the Lost rule
  above assumes settled reports keep arriving.
- [ ] M2, SelfCheck reply time: same fault as M1. From the controller host send
  the same SelfCheck request 20 times, 15 s apart, against a surviving node
  with a 4 s operation timeout and a 6 s client deadline; record each
  duration and whether the reply is BSC mode or fallback. Alternatively sample
  `/api/state` every 250 ms and take, per endpoint,
  `endpoints[].selfcheck_age_ms` right after it drops. Report min, median and
  max; the SelfCheck scheduling above assumes a median near the operation
  timeout.
- [ ] M3, BSC during a write stall: stall writes on the primary pile's storage
  without killing nodes (suspend the PDisk block devices or make them return
  write errors). Every 5 s for 2 minutes send the SelfCheck request to a node
  of a surviving pile and record: reply arrived or timed out; BSC mode or
  fallback; what it says about the primary's parts. If BSC keeps answering in
  BSC mode with GREEN parts for the stalled primary, the no-BSC survivor rule
  above is never reached in this scenario and the limit stays open; if BSC
  stops answering or marks the parts RED, it can be closed.
- [ ] M4, time to committed DISCONNECTED: default timing, `auto` mode.
  Black-hole a non-primary pile at T0 and record the time of the
  `mutation_outcome` log record with `outcome=SUCCESS` and of the generation
  bump in `/api/state`; check the 20-second default failover target for a provable failure with a
  surviving commit quorum (evidence expiry, confirmation, sampling and commit); record the measured value. Repeat with the
  primary pile black-holed. Record both as the release-note numbers.
- [ ] Measure BSC recovery/status-publication delay on a loaded cluster and
  justify `bsc_warmup_seconds`. Its 30 s default is based on a nominal 15 s
  status expiry plus 5 s publication cycle and 10 s margin, not measurements.
- [ ] Measure SelfCheck reply size and Skipper's evaluation cost per reply on a
  large degraded cluster; record them with the timing results.
- [ ] Verify exclusive administration, third-domain placement, single-node
  endpoints and emergency administrator credentials before an outage.

The classifier matches the unversioned strings
`System tablet BSC didn't provide information` and `Replication in progress`.
Pin them for every supported server build.
Connectivity does not verify survivor disks; the no-BSC survivor rule accepts
self quorum plus an own fresh non-truncated fallback reply, without a
fallback-mode RED or GREY group part attributed to the candidate. It still
needs the recordings and M3 above; this does not establish full disk health. A lost
SUSPENDED pile has no safe public recovery transition in this implementation.
This page records requirements, not a successful build or test run.

For a local mock cluster, see the [standalone mock and controller launch commands](ydbd_mock/README.md).
