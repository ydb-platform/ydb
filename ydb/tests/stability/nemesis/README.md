# Nemesis

Nemesis is a chaos testing tool for YDB clusters. It injects faults (kill nodes, block network, break disks, etc.) on a schedule and monitors cluster health through liveness and safety checks.

## What is Nemesis?

Nemesis is a distributed fault injection framework designed for stability testing of YDB clusters. It simulates real-world failures by:

- **Injecting faults** — killing processes, blocking network traffic, corrupting disks, stopping nodes, and more
- **Running on a schedule** — faults are injected automatically at configurable intervals
- **Monitoring health** — liveness checks verify the cluster is alive, safety checks detect errors in logs and metrics
- **Providing visibility** — a web UI shows active faults, execution history, and health check results

The tool runs as an orchestrator (central controller) plus per-host agents. The orchestrator plans which faults to inject and on which hosts; agents execute the faults locally and report results.

## How to Use Nemesis

### Installation

Deploy Nemesis to your cluster:

```bash
# Single-file config (cluster.yaml contains both hosts and database template)
nemesis install --yaml-config-location /path/to/cluster.yaml

# Two-file config (separate cluster.yaml and databases.yaml)
nemesis install \
    --yaml-config-location /path/to/config.yaml \
    --database-config-location /path/to/databases.yaml
```

The first host in the cluster configuration becomes the orchestrator; all other hosts become agents. Services are deployed as systemd units and started automatically.

### Accessing the UI

After installation, open the web UI:

```
http://<orchestrator_host>:31434/static/index.html
```

The UI shows:
- Available fault types grouped by category
- One scheduler, its profile and separate budgets for each pile
- Manual operations with exact targets, operation IDs and recovery progress
- Execution history and logs
- Liveness and safety check results

### Stopping Services

Stop all Nemesis services on the cluster:

```bash
nemesis stop --yaml-config-location /path/to/config.yaml
```

## High-Level Architecture

Nemesis consists of two main components:

### Orchestrator

The orchestrator is the central controller that:
- Maintains cluster state (hosts, health, active faults)
- Plans fault injection schedules
- Dispatches commands to agents via HTTP
- Runs liveness checks (cluster-wide health)
- Aggregates safety check results from agents
- Provides the web UI and HTTP API

### Agent

Each agent runs on a cluster host and:
- Receives fault injection commands from the orchestrator
- Executes faults locally (no SSH to other hosts)
- Runs safety checks on local logs and system state
- Reports execution results and health status back to the orchestrator

### Command delivery boundaries

The app constructs one `AgentExecutionService` for local actions and one
`CommandDispatcher` for orchestrator delivery/history/metrics. The boundary
scheduler and manual API share that dispatcher. `AgentCommandTransport` sends the same
explicit target to the local service or the existing agent HTTP API. Agent
selection does not change a node/slot/disk target into a whole-host action.

The transport reports acceptance, not action completion or YDB recovery. A
rejection or delivery error is reported as failed dispatch and is not retried;
a timeout alone does not prove that the agent did not apply the action. Budget
and recovery accounting belong to the shared FaultEngine for boundary/manual
execution. Local process control and service shutdown are described below.

### Execution Flow

1. The orchestrator invokes a planner on schedule or manually
2. The planner produces a list of commands (which fault type, on which host, with what payload)
3. Commands are sent to agents via HTTP `POST /api/processes`
4. Each agent executes the fault locally using `inject_fault` / `extract_fault`
5. Results are polled by the orchestrator via `GET /api/processes`
6. Liveness and safety checks run periodically to monitor cluster health

### Fault Categories

Nemesis supports several categories of fault injection:

- **Network** — isolate hosts from network, block DNS, skew time
- **Node** — kill node processes, stop/start nodes, suspend processes
- **Tablets** — kill specific tablet types, move tablets via Hive
- **Disk** — safely break/cleanup disks on nodes
- **Datacenter** — stop all nodes in a datacenter (multi-DC clusters)
- **Bridge pile** — stop all nodes in a bridge pile (bridge-enabled clusters)

## Chaos targets and failure model

Planning is entity-based (`ChaosTarget`), not host-only. Dispatch still goes to an agent host; the target says what to hit on that host (or cluster-wide for tablets).

| `target_kind` | Meaning |
|---|---|
| `host` | whole machine (time skew; network isolation if enabled) |
| `node` / `slot` / `disk` | YDB process / tenant slot / drive |
| `tablet` | Hive / tablet API (usually `guard_mode=BYPASS`) |
| `datacenter` / `pile` | topology fanout |

**Flow — boundary scheduler** (`boundary_scheduler.py`, what `deploy.py` starts and what drives scheduled chaos):

```text
tick: fill the budget up to the boundary
  → menu of (type, target) that currently fits the budget   (guard.budget_view)
  → pick uniformly → FaultEngine.inject(type, target)
      plan complete fanout → admission → guard.reserve(footprint) (atomic)
  → repeat while something fits; max_per_tick is only a burst fuse (default 16),
    tablet (BYPASS) chaos is capped separately by max_bypass_per_tick (default 1)
  → register recovery ownership → dispatch (payload includes chaos_target)
      planning/registration failure before sending releases any reservation;
      any delivery failure, including the first timeout, keeps the lease tracked
      and charged. Confirmation is paused during dispatch and requires newer HC.
  → RecoveryProbe releases the budget on healthcheck facts — never on a timer
       self    — the per-kind predicate holds (else: stuck past stuck_timeout_sec, budget kept)
       extract — dispatch the extract after auto_recovery_sec, then the same predicate
                 must confirm within confirm_timeout_sec (else: stuck, budget kept)
  → sleep base_interval ± jitter (default 15s — the healthcheck/probe cadence)
  → stop(): drains still-held toggle faults through their extract (leases move to confirm)
```

Boundary and manual calls share one app-owned `FaultEngine`. The engine reserves
before dispatch and atomically locks every target identity in a compound plan.
The scheduler only chooses candidates and controls cadence. The router validates
the HTTP request; it does not account for faults after sending. The legacy
per-type scheduler and `/api/schedule*` routes have been removed.

Manual extract requests restore through the probe for tracked toggles, avoiding a
second restore concurrent with the automatic one. They do not free a reservation:
a fresh healthcheck must confirm recovery. For a tracked toggle the response can
contain an empty `executions` list because restore belongs to the existing
operation. `force` is accepted in the manual request for compatibility but no
longer bypasses shared admission. `/api/scheduler/start`, `/api/scheduler/stop`,
`/api/problems` and the test client's protocol remain unchanged.

**Recovery predicates are per target kind** (`hc_model.py`, over the cluster-wide healthcheck each host's endpoint serves):

- `node` / `disk` — host endpoint answers **and** every pdisk/vdisk with the `"{node_id}-"` id prefix is GREEN (strict: BLUE resync / YELLOW SyncGuidRecovery block release until redundancy is restored).
- `slot` — alive compute nodes (non-empty `pools`) back to the pre-inject count. Without a fresh HC baseline the inject is skipped (boundary) or rejected with 503 (manual).
- `host` (time skew) — endpoint answers and `compute.clock_skew` is GREEN.
- `datacenter` — every DC endpoint answers and its nodes' storage is GREEN; without resolvable node ids the predicate never recovers (stuck, not silent release).

While healthcheck data is stale or no endpoint answers, the probe is *blind*: releases and stuck detection pause, but scheduled extracts still fire. Blindness is reported to `/api/problems` as `recovery_probe_blind` after the HC grace (`max_hc_age_sec`, 180s) on first boot, or at once when sight is lost; the entry resolves when sight returns. Without a probe wired, FULL-guarded types are not offered at all.

**Two independent budgets**, both from `cluster.yaml` (the erasure mode, `location.rack` / `data_center`). The erasure mode is read as `static_erasure` or `erasure` — `ydb/tools/cfg` accepts both spellings — at the top level or under `config:`, the same two places `hosts` is looked up:

- *Fail domains* — `block-4-2` ≤ 2 domains, `mirror-3-dc` = 1 full realm + 1 domain elsewhere, `none` = 0. A domain is keyed `"<data_center>/<rack>"`: rack labels are only unique inside a DC (`rack: '1'` in every DC is normal), so the realm is part of the key. Only static-node / disk / DC faults spend this budget.
- *Slots* — killing a dynamic node does not reduce storage redundancy, so it draws from its own budget: ≤ 30% of the cluster's slots down at once (`total_slots × slot_fraction`, ≥1 on small clusters). Charged by `reserve` in the shared engine for boundary/manual. `filter_safe` checks both budgets; `reserve` repeats the check atomically.

**Bridge budgets are independent per pile.** Storage fail domains are keyed by pile/DC/rack,
so repeating rack or DC labels in different piles do not merge their budgets. Each pile has
its own erasure allowance and its own slot limit (`max(1, floor(pile_slots * 0.3))` when
slots exist). An unused allowance in one pile cannot be borrowed by another. A footprint
spanning piles must fit all affected budgets atomically. `/api/scheduler` and `/api/problems`
keep the existing aggregate fields and additionally expose `failure_budget.piles`.

`TargetTopology` resolves node/slot placement from inventory, separately from the agent's
address. This also supports several processes belonging to different piles on one physical
host. Whole-host and DC/pile impacts include their affected storage domains and slots.
Bridge hosts must name a configured `location.bridge_pile_name`; unresolved resource
placement is rejected instead of using a shared fallback budget. Whole-pile loss is not
made safe merely by having another pile: it still requires a separate bridge-aware policy.

**The failure model is mandatory.** An unusable `cluster.yaml` — missing, unparsable, no recognizable erasure mode, a host without `location.rack`, or (for `mirror-3-dc`) without `location.data_center` — raises `FailureModelConfigError` and the orchestrator refuses to start. There is no unguarded mode: chaos that ignores fault tolerance is worse than no chaos.

**Catalog fields** (in `cluster_entries.py`): `target_kind`, `impact_scope`, `guard_mode` (`FULL` — filtered and accounted for; `BYPASS` — costs no budget, for tablet chaos), optional `recovery` (`extract` for faults that stay applied until extracted), `auto_recovery_sec` (toggle faults: when the probe dispatches the extract), `stuck_timeout_sec` / `confirm_timeout_sec` (probe timeouts; defaults by scope: DISK 3600s, DATACENTER 1800s, else 900s), `supports_manual`, `boundary_safe` (a custom planner must opt in before the boundary scheduler may drive it — otherwise it could inject something other than the reserved target).

**Agent contract:** node/slot/disk runners require explicit ids in `chaos_target` (`node_id`, `slot_idx`, `ic_port`) — no hostname guessing.

**UI / API:**

- History shows the target (not only host)
- Manual fault selects a type and exact inventory target, shows host/pile/DC/rack placement, and optionally sets the hold duration for this operation only. Types whose planners cannot execute an explicitly reserved target are shown as unavailable; there is no legacy fallback.
- `GET /api/inventory` — hosts/nodes/slots used for planning (built once, on the orchestrator's first request; the UI fetches it once, not on every poll)
- `GET /api/scheduler`, `POST /api/scheduler/start|stop` — boundary scheduler state and profile (`enabled`, `base_interval`, `jitter`, `max_per_tick`, `max_bypass_per_tick`). Rejected with 400: invalid values, unknown type names, and types whose planner keeps its own target state (`supports_boundary_scheduler`)
- `GET /api/problems` — nemesis-side problems: faults that never recovered (budget still held), a blind recovery probe (no fresh healthcheck data), and a degraded inventory (harness unavailable → synthesized node ids, no slot chaos). `ydb/tests/stability/tests` fetches this when disabling nemesis and attaches it to the Allure report
- The Scheduler card shows run state, profile, separate storage/slot budgets per pile, per-pile recovery counts and problems. Operations shows both manual and scheduled faults, their delivery status and recovery progress; it can filter by pile. Stale compute counts are unknown, not zero.

**Metrics (orchestrator):**

Chaos lifecycle and failure-budget leases are emitted on the orchestrator (not on agents). Scraped via monlib `/sensors` on `nemesis_mon_port` (default 8666); each transition is also logged as `nemesis_metric {json}`.

| Event | Meaning |
|---|---|
| `fault.started` / `fault.extract_dispatched` / `fault.ended` / `fault.stuck` | What was shaken, where, and when the fault opened/closed |
| `budget.acquired` / `budget.released` | When an identity entered / left the failure-model budget |
| `budget.acquire_rejected` | `reserve` refused (budget full) |

Useful sensors: `NemesisFaultActive`, `NemesisFaultActiveTotal`, `NemesisBudgetHeld`, `NemesisBudgetImpairedRacks`, `NemesisBudgetImpairedSlots`, `NemesisBudgetMaxSlots`, counters `NemesisFaultStarted` / `Ended` / `Stuck`, `NemesisBudgetAcquired` / `Released`, `NemesisFaultHoldSecondsSum` + `NemesisFaultHoldCount`. All series carry a `nemesis` label (chaos class name, or `unknown`) so Monium legends like `{{nemesis}}` resolve. Agent-side legacy counters (`InjectCompleted`, …) remain execution health only.

Each transition logs two lines on the orchestrator: a human-readable summary (`budget acquired: …`, `fault started: …`, …) and a machine-readable `nemesis_metric {json}` payload.

## Extending Nemesis

### Adding a New Fault Type

To add a new chaos scenario:

1. **Create a runner class** — inherit from `MonitoredAgentActor` and implement `inject_fault` and `extract_fault`:

```python
from ydb.tests.stability.nemesis.internal.nemesis.monitored_actor import MonitoredAgentActor

class MyCustomNemesis(MonitoredAgentActor):
    """Description of what this fault does."""

    def inject_fault(self, payload=None):
        # Execute the fault locally on this host
        # Use subprocess for system commands, or call cluster APIs
        pass

    def extract_fault(self, payload=None):
        # Restore the system to normal state
        pass
```

2. **Register the type** — add an entry to `internal/nemesis/cluster_entries.py`:

```python
def all_nemesis_type_entries() -> dict[str, dict[str, Any]]:
    out = {}
    # ... existing entries ...

    out["MyCustomNemesis"] = {
        "runner": MyCustomNemesis(),
        "schedule": 300,  # default interval in seconds
        "ui_group": "MyGroup",  # must exist in NEMESIS_UI_GROUPS
        "target_kind": TargetKind.NODE,  # what planners select
        "impact_scope": ImpactScope.NODE,  # failure-model projection
        "guard_mode": GuardMode.FULL,
        # "supports_manual": False,  # if planner.manual() is unsupported
        # Optional: specify a custom planner
        # "planner_cls": MyCustomPlanner,
        # "planner_factory": lambda key: MyCustomPlanner(key),
    }
    return out
```

3. **Add a UI group** (if creating a new category) in `internal/nemesis/catalog.py`:

```python
NEMESIS_UI_GROUPS: dict[str, dict[str, str]] = {
    # ... existing groups ...
    "MyGroup": {
        "description": "My custom fault category",
    },
}
```

4. **Re-export the runner** from `internal/nemesis/runners/__init__.py`:

```python
from ydb.tests.stability.nemesis.internal.nemesis.runners.my_module import MyCustomNemesis

__all__ = [
    # ... existing exports ...
    "MyCustomNemesis",
]
```

### Custom Planners

By default, Nemesis uses `DefaultRandomHostPlanner`, which picks one random **candidate**
(`ChaosTarget` of the type's `target_kind`) per tick. For more complex behavior, create a custom planner.

Then register it in `cluster_entries.py`:

```python
out["MyCustomNemesis"] = {
    "runner": MyCustomNemesis(),
    "schedule": 300,
    "ui_group": "MyGroup",
    "planner_cls": MyCustomPlanner,  # or use planner_factory
}
```

### Manual operation API

- `GET /api/faults` lists types, availability/reasons and recovery metadata.
- `GET /api/targets?type=<name>` lists exact inventory targets with placement.
- `POST /api/operations` accepts `{"type": "...", "target": {...}, "hold_sec": 30}`.
  `hold_sec` is optional, bounded to 1–86400 seconds and only valid for extract-mode
  faults. It never mutates a shared planner or scheduler profile. The target must
  match an inventory entry, including its port. Success returns HTTP 201 and an
  `operation_id`; delivery acceptance does not mean execution or recovery.
- `GET /api/operations` returns active operations and up to 100 recent completed
  records for this orchestrator process lifetime. Delivery failures remain visible;
  guarded faults retain recovery ownership and their budget.
- `POST /api/operations/<operation_id>/restore` requests restore of that particular
  toggle. It never resolves a new operation by target identity. Completed operations
  are no-ops; unknown IDs return 404. The budget stays held until fresh observations
  confirm recovery. Automatic and manual restore use the same probe synchronization.
- `GET /api/history` lists recent command deliveries; agent execution logs remain
  separately available in the UI.

The old manual `/api/hosts/process` adapter remains, but the UI uses operations.
The stability test protocol (`/api/scheduler/start`, `/api/scheduler/stop`,
`/api/problems`, install/stop CLI) is unchanged. Deploy the new static directory
along with the binary: the old UI's `/api/schedule*` routes no longer exist.
Catalog planner parameter schemas are internal metadata; the UI no longer rebuilds
planners from arbitrary parameters. Stateful planners must first be adapted to
explicit target admission before they can be enabled in the shared engine.

### Adding a Liveness Check

Liveness checks run on the orchestrator and verify cluster-wide health:

1. Add to `internal/orchestrator/orchestrator_warden_catalog.py`:

```python
ORCHESTRATOR_LIVENESS_CHECKS: Tuple[OrchestratorLivenessCheck, ...] = (
    # ... existing checks ...
    OrchestratorLivenessCheck(
        name="MyLivenessCheck",
        description="Check something cluster-wide",
        build=lambda cluster: MyLivenessWarden(cluster),
    ),
)
```

2. Implement the warden class:

```python
class MyLivenessWarden:
    def __init__(self, cluster):
        self.cluster = cluster

    @property
    def list_of_liveness_violations(self) -> list[str]:
        # Return a list of violation messages, or empty list if OK
        violations = []
        # ... check cluster state ...
        return violations
```

### Adding a Safety Check

Safety checks run on agents (local logs) or the orchestrator (cluster state):

**Agent-side check** (access to local logs/dmesg):

1. Add to `internal/agent/agent_warden_catalog.py`:

```python
def collect_agent_safety_check_specs(ctx: AgentSafetyContext) -> List[SafetyCheckSpec]:
    return [
        # ... existing specs ...
        SafetyCheckSpec(
            name="MyAgentSafetyCheck",
            description="Check local logs for errors",
            build_warden=lambda: MyAgentSafetyWarden(ctx.kikimr_logs_directory),
        ),
    ]
```

2. Implement the warden:

```python
class MyAgentSafetyWarden:
    def __init__(self, logs_dir):
        self.logs_dir = logs_dir

    def list_of_safety_violations(self) -> list[str]:
        # Scan logs and return violation messages
        violations = []
        # ... grep logs for error patterns ...
        return violations
```

**Orchestrator-side check** (cluster-wide):

1. Add to `internal/orchestrator/orchestrator_warden_catalog.py`:

```python
def collect_orchestrator_cluster_safety_specs(cluster) -> List[SafetyCheckSpec]:
    return [
        # ... existing specs ...
        SafetyCheckSpec(
            name="MyClusterSafetyCheck",
            description="Check cluster state",
            build_warden=lambda: MyClusterSafetyWarden(cluster),
        ),
    ]
```

2. Implement the warden:

```python
class MyClusterSafetyWarden:
    def __init__(self, cluster):
        self.cluster = cluster

    def list_of_safety_violations(self) -> list[str]:
        # Check cluster state and return violations
        violations = []
        # ... query cluster APIs ...
        return violations
```

All safety wardens must implement `list_of_safety_violations() -> list[str]`.


### Planned CMS admission (not enabled yet)

CMS admission belongs in the shared FaultEngine, before dispatch by either the
boundary scheduler or manual operations. It must not be implemented as a separate
check in each scheduler. The proposed scheduler setting is `admission_policy`:

- `local` (default): current Nemesis failure model.
- `local_and_cms`: both per-pile budgets and a CMS permission are required.
- `cms` (subsequent stage): CMS supplies availability policy; Nemesis still owns
  operation conflicts, execution tracking and recovery. Fixed local erasure limits
  must not silently prevent exercising the cluster's own failure model in this mode.

These are design options, not currently accepted configuration values.

The protocol is defined in `ydb/core/protos/cms.proto`; the HTTP handlers in
`ydb/core/cms/http.cpp` expose `/cms/api/permissionrequest` and
`/cms/api/managepermissionrequest` on the monitoring endpoint. Use real permissions,
not `DryRun`: a preflight without a retained permission cannot protect concurrent
operations. Request the complete action set with partial permission disabled and
scheduling disabled. Keep permission IDs with the operation, renew their duration
while necessary, and complete them only after recovery. Reject unused permissions
when an operation is cancelled before dispatch. Permission expiry is not evidence
that the fault has recovered.

Planning must resolve the actual resources before requesting admission: a physical
host action includes all its affected processes, whereas a node/slot action uses
its concrete YDB node identity. Disk actions require a CMS device identity before
execution; choosing a random disk on the agent afterwards is incompatible with
this contract. DC/pile actions expand to the complete set of affected resources.
The action mapping and availability mode need explicit validation against the
supported YDB versions. `FORCE_RESTART`, which bypasses checks, is not an admission
safety policy. Actions without a faithful CMS mapping are rejected when CMS is
required, including actions otherwise marked as bypassing local budgets.

Denial, partial approval, unavailable CMS or a request timeout prevent injection;
there is no automatic fallback to local policy. Restore must remain possible
without requesting another permission. Integrate this lifecycle with atomic local
reservations and shutdown, including cleanup of permissions acquired before a
failed dispatch. Do not infer absence of an effect from a dispatch timeout.

Slot recovery now captures a baseline for each affected pile. `RuntimeNodePiles`
refreshes `node ID -> pile` from the YDB monitoring endpoint
`/viewer/json/nodes?fields_required=SystemState`, using
`SystemState.Location.BridgePileName`; it does not infer a pile from the physical
host or cache dynamic IDs across restarts. The application tries the first two
configured monitoring endpoints. A deployment must expose this location field
and allow the monitoring request; an unavailable/incomplete mapping blocks bridge
slot admission and confirmation instead of falling back to the global count.
Non-bridge clusters keep the previous count-based predicate.

The healthcheck's compute nodes are partitioned using this current mapping, with
the minimum count across reporting endpoints for each pile. Growth in another
pile cannot confirm recovery. Missing bridge storage evidence also cannot confirm
a node/disk/group recovery; the affected node's storage must actually be observed
and GREEN. This is pile-level slot recovery, not proof that the same process has
returned: a replacement slot in the same pile may restore its population.

`FaultPlan` and the engine's admission extension receive the complete dispatch
plan before sending, including local-budget BYPASS faults. This extension currently
has no provider configured; it must not be mistaken for a CMS permission lifecycle.
Permission renewal/completion and the scheduler setting remain future work.
Local process discovery/control and service shutdown are implemented below.
No live cluster fault is required by these tests.


### Local ydbd process control (no harness)

Set `LOCAL_PROCESS_CONFIG` to an absolute path on the Nemesis host, or pass
`nemesis run --local-process-config /path/processes.yaml`. The existing cluster
YAML still defines erasure and bridge pile names. The process file replaces
harness inventory for local execution and gives each node/slot its own placement:

```yaml
processes:
  - host: localhost
    kind: node
    node_id: 1
    ic_port: 19001
    executable: /opt/ydb/bin/ydbd
    uid: 1000
    rack: rack-a
    datacenter: dc-a
    bridge_pile_id: 0
    restart:
      argv: [/opt/ydb/bin/ydbd, server, --node, "1", --ic-port, "19001", --yaml-config, /srv/ydb/config.yaml]
      cwd: /srv/ydb
      environment: {}
  - host: localhost
    kind: slot
    slot_idx: 0
    node_id: 1001  # optional runtime ID for a dynamic slot
    ic_port: 19002
    executable: /opt/ydb/bin/ydbd
    uid: 1000
    rack: rack-b
    datacenter: dc-b
    bridge_pile_id: 1
    restart:
      unit: ydb-slot-0.service
```

Use the complete argv, cwd, UID and environment required by the actual deployment;
these values are operator configuration, not data accepted from HTTP callers.
For non-bridge clusters omit `bridge_pile_id`. All entries describe one physical
agent host; node IDs, slot indices and interconnect ports must be unique in their
respective namespaces. Set `YAML_CONFIG_LOCATION` to the existing cluster YAML
and `MON_PORT` to its monitoring port as usual. Example foreground invocation:

```sh
YAML_CONFIG_LOCATION=/srv/ydb/cluster.yaml \
LOCAL_PROCESS_CONFIG=/srv/ydb/nemesis-processes.yaml \
NEMESIS_INSTALL_ROOT=/srv/nemesis \
nemesis run
```

For a service, put the same environment settings in its systemd configuration.
The local process file is host-specific: `install` does not distribute it.
The execution service needs permissions to signal these processes and manage the
configured systemd units (generated Nemesis units run as root).

The controller scans `/proc`, matching the executable's device/inode, UID and an
exact `--ic-port PORT` or `--ic-port=PORT` argument. It rejects missing/ambiguous
matches and already-stopped processes. Before each signal it verifies process
start time and uses Linux `pidfd_send_signal`, so PID reuse cannot redirect the
signal to an unrelated process. Processes whose port is specified only inside a
config file need an explicit ic-port argument for this discovery mechanism.

Supported local types are `KillNodeNemesis`, `KillNodeDaemonNemesis`,
`KillSlotDaemonNemesis`, `StopStartNodeNemesis` and `SuspendNodeNemesis`.
In this mode the daemon-named kill types operate on the configured ydbd process;
they do not search for or kill its launcher. Kill restarts the process immediately;
stop and suspend retain recovery ownership until extract/shutdown. Suspend uses
SIGSTOP/SIGCONT. Stop uses SIGTERM for unmanaged processes, or stops the explicitly
configured unit after checking its MainPID. Restore is idempotent.

For an existing supervisor use `restart.unit`; its MainPID must be the configured
ydbd, not a wrapper or shared supervisor. An argv recipe is for an unmanaged
process: restoration uses a new `systemd-run --service-type=exec` service with the
specified UID/cwd/environment. This puts the replacement outside the Nemesis
cgroup, so stopping Nemesis does not kill it. If a matching replacement is already
observed, it is adopted instead of launching another one. Linux pidfds and systemd
are required for this backend. Process existence after restart is distinct from
YDB readiness; the FaultEngine still waits for healthcheck recovery.

Local mode constructs only its supported planners. Host/network/time-skew, disks
and topology-group faults are rejected in this mode, including manual requests;
they cannot silently turn a process fault into an action on the entire host.
Existing liveness/safety checks retain their interfaces and cluster configuration;
this stage replaces process inventory/control, not every monitoring adapter.

### Service shutdown and restoration

`nemesis run` handles SIGTERM and SIGINT explicitly. The signal handler only wakes
the main loop; shutdown then closes admission on the FaultEngine, boundary scheduler
and local execution service. It waits for current dispatch and agent actions,
restores retained impacts, then stops background checks and the HTTP server.
`atexit` is only an idempotent fallback.

Agents record recovery ownership before invoking a runner, including an inject
that subsequently fails. A successful extract removes that record; a failed one
remains visible in the shutdown report. Automatic/manual/shutdown extracts for
the same recorded effect do not execute twice. An agent receiving SIGTERM drains
its own state, independently of the orchestrator and service stop order.

When the orchestrator stops, it calls the internal `/api/agent/drain` endpoint
concurrently on remote agents and waits for execution completion, not just command
acceptance. A new orchestrator reopens fully-drained agents through the internal
`/api/agent/resume` endpoint; an agent processing its own SIGTERM cannot reopen.
The `nemesis stop` command stops the orchestrator first while agents are available,
then stops agents. Test clients continue using the existing
`/api/scheduler/start`, `/api/scheduler/stop`, `/api/problems` and install/stop CLI.

Generated units use `TimeoutStopSec=150`, `KillMode=mixed` and the
`nemesis.service` alias. Thus `systemctl stop nemesis` first signals the Nemesis
main process, allowing its restoration helpers to finish. Update/reload the unit
when deploying this version; old installed units do not change by updating the
binary alone. The application deadline defaults to 120 seconds
(`SHUTDOWN_TIMEOUT_SEC`); keep the systemd deadline larger if changing it.

The result is saved to `$NEMESIS_INSTALL_ROOT/shutdown.json` (default
`/Berkanavt/nemesis/shutdown.json`) and failures are logged. Restore failure,
unreachable agents or deadline expiry produce an error report and a nonzero
Nemesis exit status. Successful cleanup means restore actions completed, not that
YDB has already rebuilt redundancy. SIGKILL/power loss is outside this graceful
shutdown contract; the in-memory action ledger is not a crash-recovery journal.

Tests cover real isolated child processes, precise targeting, PID-generation
checks, kill/stop/restart, suspend/resume, full local app wiring without harness,
SIGTERM during an accepted injection, duplicate restore, timeout and failure
reporting. They do not deploy a real YDB cluster or stop an installed systemd unit.
