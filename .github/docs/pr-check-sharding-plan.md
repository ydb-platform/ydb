# PR-check sharding pilot

Plan for parallel PR-check (relwithdebinfo): build once, run tests on N shards (attempt 1 only),
merge reports on a single aggregate runner, retries 2–3 on aggregate only.

**Feature flag:** `CHECKS_SWITCH.pr_check_shard_count` (integer, default / absent = legacy monolith).

## Architecture

```
check-running-allowed
        │
        ├─ [shard_count >= 2] ─────────────────────────────────────────────┐
        │   build_rwdi (graph_compare + ya make build, no -A)              │
        │        → artifacts: graph.json, context.json                       │
        │   plan_rwdi_shards → shard_plan.json + blacklist yaml per shard    │
        │   test_rwdi_shard × N (try 1 only, no PR comment / test status)    │
        │   aggregate_rwdi (merge + generate-summary + retry 2–3)            │
        │        → test_relwithdebinfo, PR comment, YDB upload               │
        │   build_and_test_asan (unchanged monolith)                         │
        │   update_integrated_status                                         │
        │                                                                    │
        └─ [shard_count < 2] ─ build_and_test matrix (legacy) ───────────────┘
```

## Progress

### Phase 1 — Tooling

- [x] Plan document
- [x] `report_utils.py` — parse build-results-report entries
- [x] `merge_build_reports.py` — merge shard reports into one JSON
- [x] `split_test_shards.py` — duration-aware bin packing → shard plan
- [x] `build_shard_blacklist.py` — complement blacklist for one shard
- [x] `extract_tests_from_graph.py` — best-effort test list from cut graph
- [x] `publish_merged_summary.sh` — summary + status from merged try1 report
- [x] Unit tests + fixtures
- [x] `test_ya` inputs: skip graph compare, suppress GitHub publish, single-try / start_retry

### Phase 2 — Workflow wiring (behind flag)

- [x] `pr_check.yml`: conditional sharded jobs when `pr_check_shard_count >= 2`
- [x] Artifact pass-through (graph, shard blacklists, shard reports)
- [x] `aggregate_rwdi` job: merge, publish try1, retry 2–3 via `test_ya`
- [x] `update_integrated_status` depends on aggregate + asan + legacy matrix

### Phase 3 — Rollout (follow-up)

- [ ] Pilot on internal PR with `pr_check_shard_count: 2`
- [ ] Compare wall-clock vs monolith (YDB `pr_check_job_runs_scatter`)
- [ ] Tune shard count, optional durations CSV from analytics
- [ ] Optional: ASAN sharding, unified S3 index for shard artifacts

## Retry policy

| Attempt | Where | ya make flags |
|---------|-------|---------------|
| 1 | N shards in parallel | cut graph, shard blacklist, build cache from `build_rwdi` |
| 2–3 | aggregate_rwdi only | `--build-only-test-deps`, muted blacklist + `--retest` on merged report |

Shards never retry. Aggregate posts `test_relwithdebinfo` (try1 green via `publish_merged_summary.sh`, retries via `test_ya`).

## Reporting

- Shards: upload `report.json` artifact only (no PR comment / test status).
- Aggregate try1 green: `publish_merged_summary.sh` → one PR comment + `test_relwithdebinfo`.
- Aggregate retry: existing `generate-summary.py` flow inside `test_ya`.
- `fail-checker.py` over merged report before retry decision.

## Enable pilot

Add to repository variable `CHECKS_SWITCH` JSON:

```json
{
  "pr_check": true,
  "pr_check_shard_count": 2
}
```

## Risks / open questions

- Graph JSON schema may change — `extract_tests_from_graph.py` is best-effort.
- Remote cache hit rate on shard runners must be validated on pilot.
- `>500` failed after try 1 skips retry (same as monolith).
