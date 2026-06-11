# PR-check sharding pilot

Plan for parallel PR-check (relwithdebinfo): build once, run tests on N shards (try 1 only),
merge reports on a lightweight job, retries 2–3 on a heavy runner only.

**Feature flag:** `CHECKS_SWITCH.pr_check_shard_count` (integer, default / absent = legacy monolith).

## Architecture

```
check-running-allowed
        │
        ├─ [shard_count >= 2] ─────────────────────────────────────────────┐
        │   build_rwdi (heavy)                                                 │
        │        graph_compare OR save-graph with save_test_graph (-A in graph)  │
        │        run_tests=false → artifacts: graph.json, context.json           │
        │   plan_rwdi_shards (tiny-worker, sparse checkout)                      │
        │        → shard_plan.json, tests.txt, debug blacklist yaml              │
        │   test_rwdi_shard × N (heavy, try 1 only)                              │
        │        explicit suite targets from plan, s3_subdir=shard_<id>          │
        │        no PR comment / test status                                     │
        │   merge_rwdi (tiny-worker) → merge + publish try1 if green / ≥500 fails  │
        │   retry_rwdi (heavy, if 1..499 fails) → ya make retries 2–3            │
        │        → test_relwithdebinfo, PR comment, YDB upload (from retry path) │
        │   build_and_test_asan (unchanged monolith)                             │
        │   update_integrated_status                                             │
        │                                                                        │
        └─ [shard_count < 2] ─ build_and_test matrix (legacy) ───────────────────┘
```

**Debug workflow** (`run_and_debug_tests.yml`, `use_sharding=true`): same shape as above with
`shard_build` / `shard_plan` / `shard_test` / `shard_aggregate_merge` / `shard_aggregate_retry`.

## Progress

### Phase 1 — Tooling

- [x] Plan document
- [x] `report_utils.py` — parse build-results-report entries
- [x] `merge_build_reports.py` — merge shard reports into one JSON
- [x] `split_test_shards.py` — duration-aware bin packing → shard plan (atomic suite dirs)
- [x] `build_shard_blacklist.py` — complement YAML (debug artifact only; not used in ya make)
- [x] `extract_tests_from_graph.py` — suite dirs from graph `module_dir` + `module_tag`
- [x] `publish_merged_summary.sh` — summary + status from merged try1 report
- [x] `render_shard_plan_summary.py` — job summary table for plan job
- [x] Unit tests + fixtures
- [x] `test_ya` inputs: `skip_graph_compare`, `save_test_graph`, suppress publish, `single_attempt` / `start_retry`
- [x] `build_and_test_ya`: `save_test_graph`, `s3_subdir`

### Phase 2 — Workflow wiring (behind flag)

- [x] `pr_check.yml`: conditional sharded jobs when `pr_check_shard_count >= 2`
- [x] Artifact pass-through (graph, shard plan, per-shard `shard_<id>.json` reports)
- [x] Split aggregate: `merge_rwdi` (tiny) + `retry_rwdi` (heavy)
- [x] Merge validates `#reports == expected_shards` before publish/retry
- [x] `update_integrated_status` depends on merge + retry + asan + legacy matrix
- [x] `run_and_debug_tests.yml`: inline sharding mode for manual smoke

### Phase 3 — Rollout (follow-up)

- [x] Manual debug smoke: **Run and debug tests** → `use_sharding=true`, olap scope (`ydb/tests/olap/`)
- [ ] Confirm shard split + S3 isolation on green smoke (distinct test counts per shard, `shard_N/try_1/` paths)
- [ ] Pilot on internal PR with `pr_check_shard_count: 2`
- [ ] Compare wall-clock vs monolith (YDB `pr_check_job_runs_scatter`)
- [ ] Tune shard count, optional durations CSV from analytics
- [ ] Optional: ASAN sharding, S3 index linking shard logs from merge summary

## Shard planning

1. **Extract** — walk graph JSON, collect `module_dir` where `module_tag` is `*test_program` (py3test, gtest, …).
2. **Scope** — filter by `--target-prefix` (e.g. `ydb/tests/olap/`).
3. **Drop scope root** — remove prefix root (e.g. `ydb/tests/olap`) when nested suites exist; avoids RECURSE parent pulling entire tree.
4. **Split** — bin-pack suite paths by estimated duration; each plan entry is one ya.make suite directory.

## Try 1 execution (per shard)

| Mechanism | Notes |
|-----------|--------|
| **build_target** | Space-separated suite paths from `shard_plan.json` for this shard id |
| **Graph** | Build saves graph for **plan only** (no `-A` on build). Shards run `ya make -A` on their suite targets **without** `--build-custom-json` |
| **save_test_graph** | Build-only mode: compile + save graph, skip test loop; graph JSON used by `extract_tests_from_graph`, not replayed on shards |
| **Blacklist** | **Not used** — `--test-blacklist-path` is ignored with `--build-custom-json` on try 1 |
| **S3** | `s3_subdir=shard_<id>` → `.../x86-64/shard_0/try_1/` (parallel shards do not overwrite) |
| **Build S3** | `s3_subdir=build` → `.../x86-64/build/` (graph save only; no `try_1` test report) |
| **Merge S3** | default `.../x86-64/try_1/` — final aggregated PR report |
| **Publish** | `publish_pr_comment=false`, `publish_github_status=false`, `single_attempt=true` |

## Retry policy

| Attempt | Where | ya make flags |
|---------|-------|---------------|
| 1 | N shards in parallel (heavy) | cut graph, explicit suite targets, build cache from build job |
| 2–3 | `retry_rwdi` only (heavy) | full checkout + graph, `--build-only-test-deps`, muted blacklist + `--retest` on merged report |

Shards never retry. `merge_rwdi` runs `publish_merged_summary.sh` when try1 is green or ≥500 failures; otherwise `retry_rwdi` owns PR comment via `test_ya`.

## Reporting

- **Shards:** GitHub artifact `shard_<id>.json`; S3 under `shard_<id>/try_1/` (logs, ya-test.html).
- **Merge try1 green / ≥500 fails:** `publish_merged_summary.sh` → PR comment + `test_relwithdebinfo` at default S3 path (`.../x86-64/try_1/`).
- **Debug merge:** same script with `SKIP_GITHUB_PUBLISH=1` → job summary table + [Merged test artifacts](S3) link; raw JSON in artifact `debug-shard-aggregate-<run_id>`.
- **Merge retry:** `generate-summary.py` inside `test_ya` on `retry_rwdi`.
- **Merge gate:** `fail-checker.py` + `#shard reports == shard_count` before publish/retry.

## Runners

| Job | Runner |
|-----|--------|
| `plan_*`, `merge_*`, `check-running-allowed`, `update_integrated_status` | `tiny-worker` |
| `build_rwdi`, `test_rwdi_shard`, `retry_rwdi`, `build_and_test_asan` | `build-preset-relwithdebinfo` |

## Enable pilot

**PR-check (after merge):** add to `CHECKS_SWITCH`:

```json
{
  "pr_check": true,
  "pr_check_shard_count": 2
}
```

**Manual debug (before merge):** GitHub Actions → **Run and debug tests** → branch `pr-check-sharding-pilot`:

| Input | Smoke value |
|-------|-------------|
| `use_sharding` | `true` |
| `shard_count` | `2` |
| `test_targets` | `ydb/tests/olap/` (or `ydb/`) |
| `increment` | `false` for fast smoke; `true` matches PR-check |
| `branches` | `pr-check-sharding-pilot` |

```bash
gh workflow run "Run and debug tests" --repo ydb-platform/ydb --ref pr-check-sharding-pilot \
  -f use_sharding=true -f shard_count=2 -f test_targets=ydb/tests/olap/ \
  -f build_preset=relwithdebinfo -f increment=false \
  -f branches=pr-check-sharding-pilot -f use_branches_config=false
```

Inspect **Plan test shards** job summary and artifact `debug-shard-plan-<run_id>`.

## Lessons / pitfalls (fixed)

- **Graph without `-A` on build:** plan reads graph JSON structure (`module_dir`), not `result` UIDs. Build graph is not replayed on shards.
- **`--build-custom-json` ignores shard targets:** both shards ran 4333 tests each (3× total work). Fixed: shards run explicit suite targets with remote cache only.
- **Blacklist + custom graph:** complement blacklist did not split work; both shards ran full scope. Fixed: explicit suite `build_target` per shard.
- **Scope root in plan:** `ydb/tests/olap` parent suite + RECURSE duplicated entire tree. Fixed: drop scope root when nested suites exist.
- **S3 collision:** parallel shards uploaded to the same `try_1/` prefix. Fixed: `s3_subdir=shard_<id>`.
- **Build job misleading S3 link:** build uploaded test-like `try_1/` and summary linked there before shards start. Fixed: `s3_subdir=build`, `GRAPH_ONLY_MODE` skips test loop, summary says "Build logs".
- **Artifact collision:** shard reports renamed to `shard_<id>.json` before upload (merge uses `shard_*.json` pattern).

## Risks / open questions

- Graph JSON schema may change — `extract_tests_from_graph.py` is best-effort on `module_dir` / `module_tag`.
- Remote cache hit rate on shard runners must be validated on full `ydb/` pilot.
- `>500` failed after try 1 skips retry (same as monolith).
- `test_size` default `small,medium` skips LARGE-only suites (e.g. `ydb/tests/olap/load`); same as PR-check, not a sharding bug.
- `build_rwdi` still posts PR comments unless `publish_pr_comment: false` is added (optional cleanup).
