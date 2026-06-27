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
- [ ] **Coverage parity:** resolve 739-row gap vs monolith (see [Coverage gap vs monolith](#coverage-gap-vs-monolith--open-investigation)) — style / peerdir / 197 real subtests; re-diff on same commit
- [ ] Tune shard count; durations now come from YDB p50 on plan (3d window) with size fallback
- [ ] Optional: ASAN sharding, S3 index linking shard logs from merge summary

## Shard planning

1. **Extract** — walk graph JSON, collect `module_dir` where `module_tag` is `*test_program` (py3test, gtest, …).
2. **Scope** — filter by `--target-prefix` (e.g. `ydb/tests/olap/`).
3. **Drop scope root** — remove prefix root (e.g. `ydb/tests/olap`) when nested suites exist; avoids RECURSE parent pulling entire tree.
4. **Split** — bin-pack suite paths by load: **p50 total suite duration** from YDB (SUM per job run, then p50); fallback to size weights (small=60, medium=600). One YDB query; `suite_folder IN (...)` only when <50 suites.

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

## Coverage gap vs monolith — open investigation

Reference comparison (not apples-to-apples on commit, but same scope `ydb/`, `test_size=small,medium`):

| Run | Workflow | Branch / commit | Report |
|-----|----------|-----------------|--------|
| [27382570708](https://github.com/ydb-platform/ydb/actions/runs/27382570708) | Run-tests (monolith) | `main` @ `69e2f3b` | try_1 `report.json` |
| [27425680676](https://github.com/ydb-platform/ydb/actions/runs/27425680676) | Run and debug tests (8 shards) | `pr-check-sharding-pilot` @ `9b85086` | merged try_1 `report.json` |

### Metrics — do not compare plan table to monolith TESTS

Three different numbers; confusing them looks like “sharding skipped half the tests”:

| Metric | Sharded run | Meaning |
|--------|------------:|---------|
| Plan `total_tests` | **37 662** | Logical tests from `ya test -L` after small+medium filter (`extract_suites_from_ya_test_list.py`); chunks listed as `… chunk]` are **not** counted |
| Plan `reported_tests` | **39 432** | Raw `Total N tests` footer from `ya test -L` before size filter (1 770 LARGE-only dropped → 37 662) |
| Merged `report.json` rows | **57 264** | Full build-results-report: every gtest subtest, py3test parametrization, peerdir pull-in, style nodes |
| Merged `type=test` rows | **51 936** | Runnable tests only (exclude `type=style`) |

Monolith try_1: **58 003** total rows (**52 133** `type=test`). Sharded merged is **−739 (−1.3%)** total, **−197 (−0.4%)** on real tests.

**Action:** in plan job summary / docs, label plan count as “logical tests (-L)” and link merged report TESTS as “executed rows”; never imply they must match.

### What was missing vs monolith (739 rows) — breakdown

Set diff: every row in monolith try_1 `report.json` absent from sharded merged try_1. Sharded is a **subset** (0 rows only-in-sharded).

#### 1. Style / lint nodes — 542 rows (`type=style`)

**What:** not runnable tests; build-graph nodes for `clang_format`, `flake8`, `clang_tidy`-style checks, template copies (`svn_interface.c`), etc.

**Why skipped:**

- Monolith runs **one** `./ya make -A ydb/` over the full dependency graph → all style nodes under `ydb/` and peerdirs land in `report.json`.
- Shards run `./ya make -A` on **explicit suite paths** from plan (779 dirs from `ya test -L`). Style nodes attached to modules **outside** those suite targets, or only reachable via full-tree `-A ydb/`, are not invoked the same way.
- Plan pipeline (`ya test -L` + `extract_suites_from_ya_test_list.py`) lists only `_RUNNABLE_TYPES` (gtest, unittest, py3test, …); style suites never enter `shard_plan.json`.

**Where (top areas among 542 missing style rows):**

| Area | Missing style rows |
|------|-------------------:|
| under `ydb/` | 408 |
| under `yql/` (peerdir) | 113 |
| under `yt/` (peerdir) | 21 |

**Open question:** are style rows required for PR-check parity, or acceptable to drop (they are not functional tests)? If required → need explicit style targets or full-graph replay per shard (likely undesirable).

#### 2. Peerdir paths outside `ydb/` — 134 rows (subset of §1, mostly style)

**What:** nodes with `path` prefix `yql/essentials/…`, `yt/python/…` pulled by monolith via `--add-peerdirs-tests all` on `-A ydb/`.

**Why skipped:**

- Plan scope is `target_prefix=ydb/`. `ya test -L ydb/` lists suites under `ydb/`; peerdir suites living under `yql/`, `yt/` are **not** in `shard_plan.json`.
- Shards still pass `--add-peerdirs-tests all`, but only when running their **listed** suite targets — peerdir tests that monolith gets “for free” from full `ydb/` root are not scheduled unless a planned suite triggers them.

**Examples from diff:** `yql/essentials/core/cbo/simple` (cbo_simple.cpp), `yql/essentials/udfs/common/…`, `yt/python/yt/yson/…`.

**Open question:** should plan add peerdir suite roots that monolith always runs, or is `-A ydb/` peerdir pull acceptable to omit for PR-check?

#### 3. Real tests (`type=test`) — 197 rows

**What:** actual test executions with `subtest_name` set — gtest cases (`BM_*` benchmarks), py3test chunks (`run[…]`, `sole chunk`), parametrized subtests.

**Why skipped (likely combined causes):**

| Cause | Explanation |
|-------|-------------|
| **Different commits** | Monolith `main@69e2f3b` vs pilot `9b85086` — tree differs; not a controlled A/B. Some of the 197 may be add/remove on branch, not sharding logic. |
| **Suite-target vs full-graph execution** | Monolith uses `--build-custom-json=graph.json` + `-A ydb/` (entire cut graph). Shards use space-separated suite paths **without** graph replay. Some subtests/chunks reachable only through full-graph scheduling may not run when only the parent suite dir is passed. |
| **Plan list vs report expansion** | `extract_suites_from_ya_test_list.py` skips lines containing ` chunk]` when counting plan tests; `ya make -A` on the suite still **expands** chunks in the report. Gap here is the opposite (report > plan), but monolith full graph may still pick up **extra** chunk/subtest rows not tied to a planned suite root. |
| **Benchmark / manual edge cases** | Missing examples include `ydb/library/actors/async/benchmark` (`BM_ManualPingActor/process_time`, …), `ydb/library/benchmarks/runner`, `ydb/library/yql/dq/comp_nodes/…/exec` py3 chunks. Worth checking SIZE(medium)/manual tags vs what `-L` lists. |

**Top suite areas among 197 missing `type=test` rows:**

| Suite area (path prefix) | Count |
|--------------------------|------:|
| `ydb/library/benchmarks/runner` | 10 |
| `ydb/core/blobstorage/vdisk` | 10 |
| `ydb/core/tx/schemeshard` | 10 |
| `ydb/core/kqp/ut` | 8 |
| `ydb/library/actors/async` | 7 |
| `ydb/library/yql/dq` | 4 |
| `ydb/tests/compatibility/*`, `ydb/tests/stress/*`, … | 2–4 each |

**Open question:** export the 197-row diff as a maintained artifact (`missing_vs_monolith.json`) and re-run diff on **same commit** after pilot merge to isolate sharding-caused gap from branch drift.

### What is NOT the cause

- **LARGE tests:** both runs use `small,medium`; 1 770 tests dropped at plan stage match `-L` size filter — same as monolith.
- **increment:** sharded smoke had `increment=false` (full suite list).
- **Failed shard 5:** merge collected all 8 shard reports; gap is not from a missing shard artifact.
- **Duplicate work on shards:** sum of per-shard reports (~95k rows) >> merged (57k) because `--add-peerdirs-tests all` duplicates peerdir rows across shards; merge deduplicates correctly.

### Follow-up tasks

- [ ] Re-run monolith vs sharded merge on **identical commit** (`main`, same `test_targets`, `test_size`, `increment`).
- [ ] Automate diff: monolith try_1 vs merged try_1 → `missing_vs_monolith.json` + summary by `type`, top-level dir, suite prefix.
- [ ] Decide product policy: **style rows**, **peerdir outside `ydb/`**, **benchmark subtests** — required for PR-check parity or explicitly out of scope.
- [ ] If real-test gap persists on same commit: try (a) replay cut graph on shards with shard-specific UID filter, or (b) expand plan with missing suite roots from diff, or (c) accept &lt;0.5% gap with documented allowlist.
- [ ] Fix plan job UX: show both “planned logical tests (-L)” and “expected report rows (historical ratio)” so smoke runs do not look under-tested.

## Stage 2: `pr_check_parallel.yml` (branch `pr-check-parallel`)

Lessons from the pilot and the two-week load analysis (shared pool of 80
machines for all presets, bimodal PR sizes, `ya test -L` ≈ 20 min on the
critical path) are implemented as a reusable workflow
`.github/workflows/pr_check_parallel.yml`:

- **`pr_check.yml` is reverted to the `main` version** — production PR-check is
  untouched until the parallel pipeline is validated.
- **Change-volume classifier** (`params` job): for PRs, changed files are
  fetched via the API; only PRs touching heavy paths (`ydb/core/`,
  `ydb/library/`, `yql/`, `util/`, `contrib/`, `build/`, …) or with ≥500
  changed files go to the sharded path. Everything else runs the classic
  single job — identical to today's PR-check, so light PRs never pay the
  sharding overhead.
- **Adaptive shard count** (`choose_shard_count.py`): the plan job estimates
  the single-job duration from history p50 suite weights
  (`D = total_weight / 60 / threads`) and picks N: `<60 min → 1`,
  `<120 → 4`, `<200 → 8`, else `12`. During shared-pool peak hours
  (09–16 UTC) N is capped at 4 so parallel checks do not starve the
  80-machine pool. `SHARD_COUNT=auto` in `plan_shard_tests.sh` enables this.
- **Pool-capacity cap** (`estimate_runner_capacity.py` +
  `.github/config/runner_capacity.yml`): before choosing N the plan step
  counts queued/in-progress jobs per `build-preset-*` runner label across
  the repo, converts them to cloud-quota demand using per-class VM
  footprints (rwdi 64c/256G, asan 96c/288G, msan/tsan 64c/320G, all
  ~2.4T NRD SSD) and subtracts demand + static reserve from the folder
  quotas (5400 vCPU / 23000G RAM / 110 VMs / 200T SSD). N is capped by how
  many more runners of the requested preset actually fit (with 10%
  headroom, floor of 2 when saturated). Quotas and footprints live in the
  config only — update there when the cloud quotas change. When the
  estimate is available it replaces the static peak-hour cap; on API
  errors the plan falls back to the peak-hour heuristic.
- **One `prepare` job instead of build/list/plan**: `ya test -L` (~20 min)
  runs in the background in a detached git worktree (so increment-mode
  `graph_compare.py` checkouts in the main tree do not race with it) while
  the cached no-tests build (~3 min) runs in the foreground; the shard plan
  is then computed from local files. Listing stays off the critical path,
  and merging the jobs saves a big-runner allocation (~6-8 runner-minutes
  with provisioning) plus two artifact round-trips. The shared `~/.ya` cache
  is cleaned once before the listing starts (`clean_ya_cache: false` on the
  build action) since ya supports concurrent invocations via file locks.
- **In-shard retries**: shards run attempts 2-3 themselves (binaries are
  already built and warm, a retry costs only the re-run time; `test_ya`
  stops retrying above 500 failures per shard). The separate cross-shard
  retry job is removed — it added a fixed ~15-35 min (runner queue, checkout,
  relink on a cold machine) to the critical path of every run with at least
  one flaky failure. Each shard uploads a final-status report
  (`merge_build_reports.py --latest-wins` over its `try_1..try_N`), and the
  merge job publishes the merged final result directly.
- **Driver for real PRs**: `run_and_debug_tests.yml` accepts a PR number or
  branch, checks out the PR merge commit (like PR-check), runs the pipeline
  with `publish=false` (no statuses/comments on the real PR) and prints a
  duration table next to the production PR-check run of the same PR for
  direct comparison.
- **Zero diff in production actions**: the pilot changes to `test_ya`,
  `build_and_test_ya` and `s3cmd` live in forked copies
  (`test_ya_parallel`, `build_and_test_ya_parallel`, `s3cmd_parallel`) used
  only by `pr_check_parallel.yml`. Production `pr_check.yml` and its actions
  are byte-identical to `main`, so the pilot can be merged into CI without
  touching the production path; fold the forks back after validation.
