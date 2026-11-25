---
name: ydb-nemesis
description: "Use for YDB Nemesis setup, fault-injection runs, recovery checks, or changes to the Nemesis framework."
---

# YDB Nemesis

Paths below are relative to the repository root. Use this skill for Nemesis (немезис) and requested YDB chaos experiments.

## Choose the workflow

1. For installation, UI/API control, fault profiles, or shutdown, read [operations](references/operations.md).
2. For a local or remote test stand, replication workload, or interpretation of results, read [experiments](references/experiments.md).
3. For framework changes, use the source map and validation below. Read the relevant component instructions before editing.

## Start with the actual environment

1. Identify the requested cluster, databases, topology, running services, binary revision, and existing run artifacts. Reuse available connection/build instructions; do not rediscover credentials or copy hosts from an old report.
2. Distinguish the distributed application in ydb/tests/stability/nemesis from the older ydb/tests/tools/nemesis/driver and ydb/tests/library/nemesis. Their commands and setup differ. A custom stand adapter can also change the catalog and API.
3. Check inventory and a fault-free workload before injecting faults. On a shared host, map each process, port, and tablet to the intended stand. A healthy agent endpoint alone does not prove YDB or data health.
4. Limit actions to the requested experiment. Set a duration or stopping condition, an explicit fault profile, and a recovery deadline. On an invariant failure or loss of fault control, stop new injections and retain evidence before deciding whether a retry is useful.
5. Track completion through fault shutdown, recovery, and the final data check. Report incomplete phases as incomplete; a started scheduler is not a completed test.

## Source map

| Responsibility | Source |
|---|---|
| Overview and extension recipes | ydb/tests/stability/nemesis/README.md |
| CLI, configuration, deployment | ydb/tests/stability/nemesis/__main__.py; ydb/tests/stability/nemesis/internal/config.py; ydb/tests/stability/nemesis/internal/orchestrator/install.py |
| Runtime wiring and HTTP contracts | ydb/tests/stability/nemesis/app.py; ydb/tests/stability/nemesis/routers/orchestrator_router.py; ydb/tests/stability/nemesis/routers/agent_router.py |
| Fault catalog, runners, target payloads | ydb/tests/stability/nemesis/internal/nemesis/cluster_entries.py; ydb/tests/stability/nemesis/internal/nemesis/catalog.py; ydb/tests/stability/nemesis/internal/nemesis/runners/ |
| Topology and failure budgets | ydb/tests/stability/nemesis/internal/orchestrator/nemesis/cluster_inventory.py; ydb/tests/stability/nemesis/internal/orchestrator/nemesis/failure_model.py |
| Scheduling and recovery | ydb/tests/stability/nemesis/internal/orchestrator/nemesis/boundary_scheduler.py; schedule_loop.py, recovery_probe.py, hc_model.py in the same directory |
| Liveness and safety registration | ydb/tests/stability/nemesis/internal/orchestrator/orchestrator_warden_catalog.py; ydb/tests/stability/nemesis/internal/agent/agent_warden_catalog.py |
| Unit tests | ydb/tests/stability/nemesis/ut/ya.make |

## Change and validate the framework

1. Trace a fault through catalog → planner/ChaosTarget → dispatch → runner → recovery predicate. Keep node/slot/disk IDs explicit; do not substitute guessed node IDs for missing inventory.
2. Keep failure-budget accounting through partial dispatch and recovery confirmation. Inspect both schedulers and manual API paths when changing this contract. Elapsed recovery time alone does not release a lease.
3. Use the README extension recipe for new runners, planners, and wardens. Select target_kind, impact_scope, guard_mode, recovery, and boundary_safe deliberately; a custom planner must honor the reserved target before opting into the boundary scheduler.
4. Start framework validation with the small unit target, following the active build-environment instructions:

   ```bash
   set -o pipefail
   ./ya make --build relwithdebinfo -tA ydb/tests/stability/nemesis/ut 2>&1 | tail -n 80
   ```

5. For deployment integration, inspect ydb/tests/library/stability/deploy.py and ydb/tests/stability/tests. Run cluster-dependent tests only when that experiment is requested. Documentation-only changes need instruction/link checks, not a live fault campaign.
