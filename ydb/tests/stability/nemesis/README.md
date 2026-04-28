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

By default, Nemesis uses `DefaultRandomHostPlanner` which injects faults on a random host per tick. For more complex behavior, create a custom planner.

Then register it in `cluster_entries.py`:

```python
out["MyCustomNemesis"] = {
    "runner": MyCustomNemesis(),
    "schedule": 300,
    "ui_group": "MyGroup",
    "planner_cls": MyCustomPlanner,  # or use planner_factory
}
```

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
