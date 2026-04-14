# Nemesis App

Fault injection application for YDB clusters.

## Operating Modes

- **orchestrator** — chaos scenario planning, dispatch to agents, UI, liveness/safety wardens on the orchestrator.
- **agent** — runs nemesis runners on the host, local safety checks via logs; process state is polled from the orchestrator over HTTP.

## Configuration

Environment variables (or CLI arguments for `install` / `run`, see `nemesis --help`):

| Parameter | Description | Default |
|-----------|-------------|---------|
| `NEMESIS_TYPE` | Mode: `orchestrator` or `agent` | `orchestrator` |
| `APP_HOST` | HTTP bind address | `::` |
| `APP_PORT` | Application port | `31434` |
| `MON_PORT` | Monitoring port (warden / health) | `8765` |
| `STATIC_LOCATION` | UI static files directory (orchestrator) | `static` |
| `YAML_CONFIG_LOCATION` | Path to `cluster.yaml` | — |
| `NEMESIS_INSTALL_ROOT` | Installation root on remote hosts (`rsync`, `ExecStart` in unit) | `/Berkanavt/nemesis` |
| `KIKIMR_LOGS_DIRECTORY` | Kikimr logs directory for agent safety wardens | `/Berkanavt/kikimr/logs/` |

For installation with non-standard paths:

```bash
./nemesis install --yaml-config-location /path/to/cluster.yaml \
  --install-root /opt/nemesis \
  --kikimr-logs-directory /var/log/kikimr/
```

## Cluster Installation and Startup

```bash
# From the directory containing the nemesis binary
./nemesis --yaml-config-location /your/path/to/config.yaml install
```

The first host from `cluster.yaml` becomes the orchestrator, the rest become agents.

Default port: **31434** (configurable via `APP_PORT`).

## Stopping Services

```bash
./nemesis stop
```

## `internal/` Structure

- **Shared** (both orchestrator and agent): `config.py`, `models.py`, `event_loop.py`, `nemesis/catalog.py`, `nemesis/chaos_dispatch.py`.
- **`internal/nemesis/runners/`** — all runner classes (nemesis actors). `__init__.py` re-exports all classes for convenient imports.
- **`internal/nemesis/cluster_entries.py`** — cluster nemesis entries (tablet kills, daemon kills, disk ops, datacenter/bridge pile), extracted from `catalog.py` for readability.
- **`internal/nemesis/catalog.py`** — main registry `NEMESIS_TYPES`, UI groups, `build_all_planners()` and API helpers. Imports core runners from `runners/` and cluster entries from `cluster_entries.py`.
- **`internal/agent/`** — agent only: `agent_warden_checker.py`, `nemesis/runner.py` (`NemesisManager`).
- **`internal/orchestrator/`** — orchestrator only: `install.py`, `orchestrator_warden_checker.py`, `nemesis/` (scheduling, `chaos_state`, planners). Orchestrator state (hosts, healthcheck, chaos store) lives in `routers/orchestrator_router.py`.

## UI and API Models

Static responses for the UI are described by dataclasses in `internal/models.py` (`ProcessInfo`, `ProcessTypeRow`, `WardenCheckReport`, etc.). Endpoints return the same fields as before, as JSON.

## Agent Execution Results

Process completion and logs on the agent are **not pushed** to the orchestrator. State is retrieved by polling from the orchestrator: `GET /api/hosts/processes` (aggregates `GET /api/processes` across hosts).

## Nemesis Runner Logging

Messages from nemesis runners executing on the agent are written to the `ydb.tests.stability.nemesis.execution` logger. `NemesisManager` attaches a thread-local handler to it during execution for collecting logs in the UI; **the root logger is not modified**.

## Browser UI

`http://<orchestrator_host>:31434/static/index.html`

---

## Extension: Custom Nemesis

Registry and UI groups: **`internal/nemesis/catalog.py`** (`NEMESIS_TYPES`, `NEMESIS_UI_GROUPS`).
Cluster nemesis entries: **`internal/nemesis/cluster_entries.py`** (`all_nemesis_type_entries()`).
All runner classes are re-exported from **`internal/nemesis/runners/__init__.py`**.

### How Nemesis Execution Works

1. The **orchestrator** invokes the planner on schedule or manually (`ChaosOrchestratorStore` → `NemesisPlannerBase`), obtaining a list of `DispatchCommand`.
2. Commands are sent to agents: **HTTP `POST /api/processes`** with body `{ type, action, payload }` (see `internal/orchestrator/nemesis/schedule_loop.py`, `chaos_dispatch.py`).
3. The **agent** in `routers/agent_router.py` takes the `runner` from `NEMESIS_TYPES[type]` and runs **`inject_fault` / `extract_fault`** in a thread via `NemesisManager` (`internal/agent/nemesis/runner.py`).

The scenario body always runs on the **agent**; the orchestrator only plans **which** type, on **which** host, and **what** payload.

### Local Execution Principle

All runners perform fault injection **locally** on the agent host via `subprocess` (systemctl, kill, iptables, ip route, etc.). **SSH to remote hosts is not used** — the orchestrator is responsible for selecting the target host and dispatching the command to the appropriate agent.

| Runner Category | Planner | What the Agent Does |
|-----------------|---------|---------------------|
| Single host (kill node/slot, stop/start, suspend, rolling update) | `DefaultRandomHostPlanner` / `PinnedFirstHostPlanner` / `SerialStaggeredInjectPlanner` | Kills / stops / signals the **local** kikimr process |
| Datacenter (stop nodes, iptables, route unreach) | `DataCenterFanoutPlanner` — dispatches inject to **all** hosts in the selected DC | Each agent stops / blocks its **own** local services |
| Bridge pile (stop nodes, iptables, route unreach) | `BridgePileFanoutPlanner` — dispatches inject to **all** hosts in the selected pile | Each agent stops / blocks its **own** local services |
| Cluster API (tablet kill, hive, disk break) | `DefaultRandomHostPlanner` | Calls cluster gRPC / HTTP API (no SSH) |

### Registering a Type

1. Add an actor class inheriting from **`MonitoredAgentActor`** (like `NetworkNemesis` / `KillNodeNemesis`): implement **`inject_fault`** and **`extract_fault`**, reading `payload` from dispatch if needed.
2. Create a **string id** for the process (a constant, like `NETWORK_NEMESIS` in `network_planner.py`).
3. In **`NEMESIS_TYPES`** add an entry:
   - **`runner`**: actor instance;
   - **`schedule`**: default interval for the UI (seconds);
   - **`ui_group`**: group id in **`NEMESIS_UI_GROUPS`** (description for `/api/process_types/grouped`; an unknown group will fall under "Other" if no description is added);
   - **`planner_cls`**: planner class **or omit the key** (see below).

### Without a Custom Planner

**Do not specify `planner_cls`** in the `NEMESIS_TYPES` entry. Then `build_all_planners()` will use **`DefaultRandomHostPlanner`** (`internal/orchestrator/nemesis/default_planner.py`):

- on each schedule tick a **random** host from the cluster is selected and an **inject** with an **empty** `PAYLOAD_INJECT` is sent;
- when the schedule is **disabled**, **extract is not planned for the list of affected hosts** (the planner does not "remember" anyone);
- manual inject/extract from the UI still goes to the selected host.

This is sufficient if the scenario has **no memory between ticks** and **no bulk extract** when the schedule is disabled (e.g., a one-shot hit on a random node with an empty payload, if the actor does everything locally).

### With a Custom Planner

Needed when, for example:

- you need to track a **set of affected hosts** and upon **disabling** the schedule perform **extract on all of them**;
- a single tick requires **multiple** commands or **custom** host selection logic (not "one random");
- **different** `PAYLOAD_INJECT` / `PAYLOAD_EXTRACT` (like the network nemesis).

Steps:

1. Subclass **`NemesisPlannerBase`** (`internal/orchestrator/nemesis/nemesis_planner_base.py`): set **`nemesis_type`**, **`PAYLOAD_INJECT`**, **`PAYLOAD_EXTRACT`**, implement **`scheduled_tick`**, **`_drain_tracked_hosts`**, **`_register_inject`**, **`_register_extract`** (reference — `network_planner.py`, `kill_node_planner.py`).
2. In **`NEMESIS_TYPES`** specify **`planner_cls`: YourPlanner`** (class, not instance — it is created by `build_all_planners()`).

### Summary: When to Use the Default Planner

| Requirement | `DefaultRandomHostPlanner` sufficient (no `planner_cls`)? |
|-------------|----------------------------------------------------------|
| One inject to a random host per tick, payload is irrelevant / fixed in the actor | Yes |
| Remember "who was affected" and extract all when schedule is disabled | No, custom planner needed |
| Non-standard host selection / multiple commands per tick | No, custom planner needed |

---

## Extension: Liveness and Safety Checks

Unified safety check registration interface — **`SafetyCheckSpec`** (`internal/safety_warden_execution.py`). Both agent and orchestrator use the same dataclass and the same execution pipeline:

```
specs → collect_safety_warden_pairs(specs) → build_safety_runs(specs) → run_in_executor
```

Catalogs: **`internal/agent/agent_warden_catalog.py`** (`collect_agent_safety_check_specs`), **`internal/orchestrator/orchestrator_warden_catalog.py`** (`collect_orchestrator_cluster_safety_specs`). The list of checks is not exposed in the UI/API before execution — rows appear in **`GET /api/hosts/warden/results`** after **`Run Checks`**.

### SafetyCheckSpec

```python
@dataclass(frozen=True)
class SafetyCheckSpec:
    name: str
    description: str = ""
    build_pairs: Optional[Callable[[], List[Tuple[str, Any]]]] = None   # factory → multiple wardens
    build_warden: Optional[Callable[[], Any]] = None                    # single warden
```

Specify **exactly one** of `build_pairs` / `build_warden`:

- **`build_pairs`** — factory returning `[(slot_name, warden), ...]`. Used for agent log factories that produce multiple wardens at once.
- **`build_warden`** — returns a single warden; the spec's `name` is used as `slot_name`.

Both kinds of wardens must implement `list_of_safety_violations() -> list`.

### Where Each Check Runs

| Category | Where it runs | How it appears in the report |
|----------|---------------|------------------------------|
| **Liveness** | **Orchestrator** only: subprocess `nemesis liveness` (set from `ORCHESTRATOR_LIVENESS_CHECKS`, execution via `run_orchestrator_liveness_cli_batch` in `orchestrator_warden_execution.py`) | `_orchestrator` in `GET /api/hosts/warden/results` |
| **Safety (agent)** | Each **agent** locally (`AgentWardenChecker`, background asyncio + `run_in_executor`, checks run in parallel). Specs from `collect_agent_safety_check_specs(ctx)` | Per host in the same JSON |
| **Safety (orchestrator cluster)** | **Orchestrator** (`OrchestratorWardenChecker`): specs from `collect_orchestrator_cluster_safety_specs(cluster)`, same `build_safety_runs` pipeline | In `_orchestrator.safety_checks` |
| **Safety (orchestrator aggregated)** | **Orchestrator**: tuple `ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS` (cross-agent aggregation — `unified_agent_verify_failed_aggregated.py`) | In `_orchestrator.safety_checks` |

Agents **do not run liveness checks** (the liveness block in the per-host report is empty).

### Adding a Liveness Check

1. In **`internal/orchestrator/orchestrator_warden_catalog.py`** add an element to the **`ORCHESTRATOR_LIVENESS_CHECKS`** tuple: an **`OrchestratorLivenessCheck`** with **`name`**, **`description`**, **`build=lambda c: ...`**.
2. The **`nemesis liveness`** command calls **`run_orchestrator_liveness_cli_batch`** — no need to duplicate the list.

Execution: the binary on the orchestrator calls `nemesis liveness`, which uses the same catalog internally.

### Adding a Safety Check

Depends on the **location** (`agent` / `orchestrator`).

**Agent** — check with access to **local** logs / dmesg, etc.:

1. In **`internal/agent/agent_warden_catalog.py`** add a `SafetyCheckSpec` to the list returned by **`collect_agent_safety_check_specs(ctx)`**.
2. For a factory producing multiple wardens, use **`build_pairs`** (see `kikimr_start_logs_safety_warden_factory`).
3. For a single warden, use **`build_warden`** (see `UnifiedAgentVerifyFailedSafetyWarden`).

```python
SafetyCheckSpec(
    name="my_new_check",
    description="Description for logs",
    build_warden=lambda: MyNewSafetyWarden(...),
)
```

**Orchestrator (cluster)** — cluster-wide check (PDisks, tablets, etc.):

1. In **`internal/orchestrator/orchestrator_warden_catalog.py`** add a `SafetyCheckSpec` to the list returned by **`collect_orchestrator_cluster_safety_specs(cluster)`**. The cluster is captured by the closure in `build_warden`.

```python
SafetyCheckSpec(
    name="MyClusterCheck",
    description="Check something cluster-wide",
    build_warden=lambda: MyClusterSafetyWarden(cluster, timeout_seconds=30),
)
```

**Orchestrator (aggregated)** — aggregation of agent safety responses:

1. Add an element to **`ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS`** with **`agent_source_class_name`** and **`impl`**. Agent waiting — **`OrchestratorWardenChecker._wait_for_agent_safety_completion_async`**, invocation — **`run_orchestrator_aggregated_safety`**.

For new aggregators: in **`safety_checks`** search for the row by **`name`** (exact match or first token — see **`UnifiedAgentVerifyFailedAggregated._row_matches_class`**).
