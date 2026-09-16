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
- Active schedules and their intervals
- Manual inject/extract controls
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
  → pick uniformly, guard.reserve(footprint)                 (atomic, held until the probe)
  → repeat while something fits; max_per_tick is only a burst fuse (default 16),
    tablet (BYPASS) chaos is capped separately by max_bypass_per_tick (default 1)
  → plan_inject → dispatch (payload includes chaos_target)
      on failure: release the lease only if nothing dispatched;
      a partial fanout keeps the budget charged and stays tracked
  → RecoveryProbe releases the budget on healthcheck facts — never on a timer
       self    — the per-kind predicate holds (else: stuck past stuck_timeout_sec, budget kept)
       extract — dispatch the extract after auto_recovery_sec, then the same predicate
                 must confirm within confirm_timeout_sec (else: stuck, budget kept)
  → sleep base_interval ± jitter (default 15s — the healthcheck/probe cadence)
  → stop(): drains still-held toggle faults through their extract (leases move to confirm)
```

**Flow — legacy per-type schedule** (`schedule_loop.py`, still available via `/api/schedule/*`):

```text
ClusterInventory.entities(kind)
  → FailureModelGuard.filter_safe(...)
  → planner.scheduled_tick(candidates)
  → dispatch → record_inject (held) + RecoveryProbe.track with the same per-kind predicate
     (slot injects without a fresh HC baseline are skipped, same as the boundary scheduler)
```

There is **no** dispatch-time `can_inject` veto. Safety is plan-time (`filter_safe` / `fits`) plus accounting after dispatch (`record_inject` / `reserve`).

**Recovery predicates are per target kind** (`hc_model.py`, over the cluster-wide healthcheck each host's endpoint serves):

- `node` / `disk` — host endpoint answers **and** every pdisk/vdisk with the `"{node_id}-"` id prefix is GREEN (strict: BLUE resync / YELLOW SyncGuidRecovery block release until redundancy is restored).
- `slot` — alive compute nodes (non-empty `pools`) back to the pre-inject count. Without a fresh HC baseline the inject is skipped (boundary / legacy) or rejected with 503 (manual).
- `host` (time skew) — endpoint answers and `compute.clock_skew` is GREEN.
- `datacenter` — every DC endpoint answers and its nodes' storage is GREEN; without resolvable node ids the predicate never recovers (stuck, not silent release).

While healthcheck data is stale or no endpoint answers, the probe is *blind*: releases and stuck detection pause, but scheduled extracts still fire. Blindness is reported to `/api/problems` as `recovery_probe_blind` after the HC grace (`max_hc_age_sec`, 180s) on first boot, or at once when sight is lost; the entry resolves when sight returns. Without a probe wired, FULL-guarded types are not offered at all.

**Two independent budgets**, both from `cluster.yaml` (the erasure mode, `location.rack` / `data_center`). The erasure mode is read as `static_erasure` or `erasure` — `ydb/tools/cfg` accepts both spellings — at the top level or under `config:`, the same two places `hosts` is looked up:

- *Fail domains* — `block-4-2` ≤ 2 domains, `mirror-3-dc` = 1 full realm + 1 domain elsewhere, `none` = 0. A domain is keyed `"<data_center>/<rack>"`: rack labels are only unique inside a DC (`rack: '1'` in every DC is normal), so the realm is part of the key. Only static-node / disk / DC faults spend this budget.
- *Slots* — killing a dynamic node does not reduce storage redundancy, so it draws from its own budget: ≤ 30% of the cluster's slots down at once (`total_slots × slot_fraction`, ≥1 on small clusters). Charged by `reserve` (boundary scheduler) and by `record_inject` (legacy loop, manual inject). The one place that ignores it is the `filter_safe` pre-check: a slot candidate carries no fail domain, so it is always admitted there — `reserve` is what actually refuses.

**The failure model is mandatory.** An unusable `cluster.yaml` — missing, unparsable, no recognizable erasure mode, a host without `location.rack`, or (for `mirror-3-dc`) without `location.data_center` — raises `FailureModelConfigError` and the orchestrator refuses to start. There is no unguarded mode: chaos that ignores fault tolerance is worse than no chaos.

**Catalog fields** (in `cluster_entries.py`): `target_kind`, `impact_scope`, `guard_mode` (`FULL` — filtered and accounted for; `BYPASS` — costs no budget, for tablet chaos), optional `recovery` (`extract` for faults that stay applied until extracted), `auto_recovery_sec` (toggle faults: when the probe dispatches the extract), `stuck_timeout_sec` / `confirm_timeout_sec` (probe timeouts; defaults by scope: DISK 3600s, DATACENTER 1800s, else 900s), `supports_manual`, `boundary_safe` (a custom planner must opt in before the boundary scheduler may drive it — otherwise it could inject something other than the reserved target).

**Agent contract:** node/slot/disk runners require explicit ids in `chaos_target` (`node_id`, `slot_idx`, `ic_port`) — no hostname guessing.

**UI / API:**

- History shows the target (not only host)
- Manual Run lists nodes/slots when `supports_manual` and `target_kind` need them; types with `supports_manual: false` (network, time skew, rolling restart, bridge pile) are schedule-only
- `GET /api/inventory` — hosts/nodes/slots used for planning (built once, on the orchestrator's first request; the UI fetches it once, not on every poll)
- `GET /api/scheduler`, `POST /api/scheduler/start|stop` — boundary scheduler state and profile (`enabled`, `base_interval`, `jitter`, `max_per_tick`, `max_bypass_per_tick`). Rejected with 400: invalid values, unknown type names, and types whose planner keeps its own target state (`supports_boundary_scheduler`)
- `GET /api/problems` — nemesis-side problems: faults that never recovered (budget still held), a blind recovery probe (no fresh healthcheck data), and a degraded inventory (harness unavailable → synthesized node ids, no slot chaos). `ydb/tests/stability/tests` fetches this when disabling nemesis and attaches it to the Allure report
- The UI's Nemesis Scheduler card shows run state, profile, both budgets, the recovery probe and the problem list; the per-type schedule toggles in the accordion belong to the legacy loop

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

### Parameterized Planners

A nemesis entry may declare a `params` schema. When present, the UI replaces the simple toggle with a "Run" button that opens a modal asking the user for parameter values; those values are sent to `POST /api/schedule` and used to rebuild the planner before the schedule is enabled. Planners that do not declare `params` keep the default behaviour.

Schema format (each item is a field rendered in the UI):

```python
{
    "name": "nodes_per_step",       # kwarg passed to the planner factory
    "label": "Nodes per step",      # human-readable label in the UI
    "type": "int",                  # "int" | "float" | "bool" | "string"
    "default": 2,                   # initial value shown in the modal
    "min": 1, "max": 64,            # optional, used for number inputs
    "description": "How many nodes to restart per tick",  # optional tooltip
}
```

The factory must accept `params` (a `dict[str, Any] | None`); the catalog auto-detects this via `inspect.signature` for backwards compatibility with factories that only take `nemesis_type_key`.

Example — `ClusterRollingRestartNemesis`:

```python
out["ClusterRollingRestartNemesis"] = {
    "runner": ClusterRollingRestartNemesis(),
    "schedule": 600,
    "ui_group": "Node",
    "planner_factory": lambda key, params=None: RollingRestartNemesisPlanner(**(params or {})),
    "params": [
        {"name": "nodes_per_step",    "label": "Nodes per step",      "type": "int",  "default": 2,     "min": 1},
        {"name": "use_storage_nodes", "label": "Use storage nodes",   "type": "bool", "default": False},
        {"name": "node_downtime_sec", "label": "Node downtime (sec)", "type": "int",  "default": 60,    "min": 1},
    ],
}
```

Parameters of `RollingRestartNemesisPlanner`:

- `nodes_per_step` (`int`, default `2`) — how many cluster nodes are restarted within one scheduled tick.
- `use_storage_nodes` (`bool`, default `False`) — when `True`, the planner targets storage nodes; when `False`, compute nodes.
- `node_downtime_sec` (`int`, default `60`) — how long the agent keeps each node stopped before starting it back via systemd. Forwarded to the agent as `duration` in the dispatch payload.

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
