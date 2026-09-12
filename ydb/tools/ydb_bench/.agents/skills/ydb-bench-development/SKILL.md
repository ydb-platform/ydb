---
name: ydb-bench-development
description: "Develop, debug, review or operate ydb/tools/ydb_bench: configuration, execution, results, federation, placement templates and embedded web UI. Not for arbitrary YDB performance investigations."
---

# YDB Benchmark Development

Resolve the checkout and current diff. Read the relevant section of
[README.md](../../../README.md), then trace the changed value from parser or
HTTP request through execution, storage and UI. A visible control alone does
not prove that execution uses its value.

For frontend changes read [UI conventions](references/ui.md).
For tests, builds or deployment read [validation and operation](references/validation.md).

## Architecture

Paths are relative to `ydb/tools/ydb_bench/`.

| Area | Source and responsibility |
|---|---|
| Entry/package | `__main__.py`, `lib/cli.py`, root `ya.make`: CLI and bundled executable resources |
| Configuration | `benchmarks/registry.py`, benchmark descriptors, `lib/config.py`: schema, validation, normalized profiles and run plan |
| Execution | `lib/runner.py`: subprocess groups, cancellation and affinity; `lib/actors_core.py`, `lib/local_ydb.py`, `lib/local_ydb_workloads.py`: benchmark-specific lifecycle |
| Search | `lib/load_control.py`: observations and load search; `lib/local_ydb.py`: measurement, verification and geometry scaling |
| Results | `lib/results.py`, `lib/common.py`, `lib/import_results.py`: manifests, atomic writes and imports |
| Web | `lib/web.py`: RunService, queue/lifecycle, HTTP handlers, embedded JS/CSS, reports and comparisons |
| Federation | `lib/hosts.py`: identities, membership, tokens and peer allowlists; `lib/federation.py`: read-through aggregation and host-qualified references |
| Topology/metrics | `lib/topology.py`: discovery and placement; `lib/system_info.py`, `lib/linux_telemetry.py`, `lib/ydb_telemetry.py`: system/process/YDB measurements |
| Templates | `lib/cluster_templates.py`: validated revisioned specifications; `lib/cluster_templates_ui.py`: placement views and joint previews |

Web assets are Python strings served by the executable, not a separately
deployed frontend. Editing sources does not update a running binary. Register
new modules in the relevant `ya.make`.

## Boundaries

- Template CRUD describes placement; it does not launch a distributed cluster.
  Physical host, logical DC/rack/body and tenant are independent dimensions.
  CLI generators have only physical placement; static nodes are shared
  infrastructure; dynamic nodes can belong to tenants.
- Actor-system settings and vCPU belong to run configuration, not templates.
  Affinity is an allowed CPU mask, not pool sizing. Shared chiplet/NUMA masks
  may exceed accounting reservations; reservations are not enforced quotas.
- Joint previews account for other nodes on the same host, preserve manual
  constraints and distinguish reservation slots from actual masks. Do not
  silently change strategy meaning or treat a preview as runtime evidence.
- Browser access to other hosts goes through the current server. Preserve peer
  allowlists and host-qualified IDs through selection, persistence, baselines
  and deep links. Results remain on their owning hosts. Report partial
  federation failures instead of presenting an empty successful response.
- Keep Builder, YAML, validation and saved configuration aligned. Check old
  inputs, defaults and report consumers when changing fields. The configured
  percentile, not a hard-coded label, determines the latency metric.
- Predictions are not measured search evidence. Verification may reject a
  boundary. A passing maximum is a lower bound, and throughput alone does not
  establish SLO success.
- Preserve cancellation and cleanup. Read `model/README.md` and
  `model/run_lifecycle.pml` when changing run/queue lifecycle semantics.

Update guidance when a documented contract changes. Keep machine aliases,
credentials, deployment paths and transient run IDs out of repository skills.
