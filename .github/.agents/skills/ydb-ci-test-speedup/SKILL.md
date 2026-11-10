---
name: ydb-ci-test-speedup
description: >-
  Cold PR-check A/B (increment/cache off), baseline/treatment draft PRs, and
  measured/PSI cgroup dashboards for CI wall-time experiments under .github.
  Use for «протестируй ускорение», no-cache PR-check, or Delayed-by / PSI reads.
---

# CI test speedup (cold A/B + measured lab)

Research tooling (custom ya-bin + per-test cgroup dashboard) lives on
`dev/ci-measured-lab`. Do not merge that raw stack to `main` without an
explicit opt-in plan.

## Hard rules

1. Baseline and treatment use the **same** cold harness (identical `.github`
   vs the shared base SHA).
2. Never compare a cold treatment to a warm/default PR-check.
3. Isolated branch/worktree names; do not reuse another agent's trees.
4. `SIZE(MEDIUM)` → `REQUIREMENTS(cpu)` **≤ 4**; `SIZE(SMALL)` → **≤ 1**.
5. `SPLIT_FACTOR` requires `FORK_TESTS()` or `FORK_SUBTESTS()`.
6. A missing/skipped suite is not a speedup — diff launched tests first.
7. Local admission for tests is `test` + `cpu` slots only. Cap concurrent
   disk hogs with higher `REQUIREMENTS(cpu)`, a separate job, or lower
   `--test-threads` — not with invented disk/LPT flags.

## 1. Worktrees

```bash
cd /Users/kirrysin/projects/ydb   # or active ydb root
git fetch origin main
BASE_SHA=$(git rev-parse origin/main)
BASE=/tmp/ydb-<experiment>-baseline
TREAT=/tmp/ydb-<experiment>-treatment
git worktree add -b dev/<experiment>-baseline "$BASE" "$BASE_SHA"
git worktree add -b dev/<experiment>-treatment "$TREAT" "$BASE_SHA"
```

Pick a unique `<experiment>`. Record `BASE_SHA`.

## 2. Cold harness (both trees)

Required for draft PR A/B: the PR job call site in
`.github/workflows/pr_check.yml` (`build_and_test`). Also patch the push /
postcommit call site when you want a clean whole-file `.github` identity
diff between trees.

Also edit `.github/actions/build_and_test_ya/action.yml`.

| Knob | Warm (default) | Cold A/B |
|------|----------------|----------|
| `increment` | `true` | `false` |
| `put_build_results_to_cache` | `true` | `false` |
| `additional_ya_make_args` | `-DDEBUGINFO_LINES_ONLY` | `--no-bazel-remote-store -DDEBUGINFO_LINES_ONLY` |
| `REMOTE_CACHE_URL` in `vars` | `vars.REMOTE_CACHE_URL_YA` | `""` |

In `action.yml` (belt-and-suspenders; workflow knobs alone are the cold path
used by `pr_like_test_benchmark.yml`):

1. Replace `if [ "${{ inputs.increment }}" = "true" ]; then` with
   `if false; then` so the job never takes the `graph_compare` path and
   always runs configured targets with `--retest`.
2. Prefer empty `REMOTE_CACHE_URL` from the table. Optionally also force
   `params+=(--no-bazel-remote-store)` so a non-empty URL cannot re-enable
   the store.

Prove harness identity:

```bash
diff -u <(git -C "$BASE" diff "$BASE_SHA" -- .github) \
        <(git -C "$TREAT" diff "$BASE_SHA" -- .github)
# must be empty
```

## 2b. Optional measured overlay (both trees, identical)

After the cold harness, merge or cherry-pick `origin/dev/ci-measured-lab`
into **both** trees if you need per-test cgroup / PSI / Delayed-by artifacts.

Then:

1. Re-apply any cold `pr_check.yml` / `action.yml` bits the lab tip overwrote
   (lab tip may still have a remote-cache block; keep cold knobs).
2. Re-run the `.github` identity diff — must stay empty between trees.

Custom ya helpers (`.github/scripts/custom_ya_launch.sh`,
`.github/scripts/ya_cgroup_exec.sh`) and `custom_ya_bin_s3_uri` exist on the
lab tip, not on `main`.

## 3. Treatment only

Speedup edits only in `$TREAT` (suite code / `ya.make` / docs). Do not edit
`.github` after step 2 / 2b except to keep baseline and treatment `.github`
identical.

## 4. Draft PRs

1. Commit + push baseline (`.github` only, plus shared measured overlay if used).
2. Commit + push treatment (same `.github` + code).
3. Open **draft** PRs against `main`; cross-link; put `BASE_SHA` in the body.
4. Titles/bodies must say cold A/B: increment off, cache off.

If GraphQL `gh pr create` fails with Forbidden, use REST
`gh api repos/ydb-platform/ydb/pulls -X POST`.

Open drafts still run PR-check when `pull_request_target` fires
(`state == open`). Other gates (`CHECKS_SWITCH`, ownership, mergeability) can
still skip or fail the job.

## 5. Wait and compare

PR-check builds a **merge commit** with current `main`. If `main` moved between
the two runs, rebase both onto one SHA and re-run before comparing.

Use **try_1** test metrics, not the Actions job clock. Artifact URLs, metric
formulas, PSI / Delayed-by: `references/measured-metrics.md`.

Validity before claiming a win:

1. Same merge-base / comparable `main` for both runs.
2. Diff suite/test names in both `try_1/report.json` — fewer tests ≠ faster.
3. Scan configure / FAILED (example: `SPLIT_FACTOR` without `FORK_*`).

## Related

- `ydb/agents/TESTS.md`, root `AGENTS.md`
- `ydb/agents/TEST_CPU_INEFFICIENCIES.md` (when present on the branch)
- Dashboard builders: `.github/scripts/utils/dashboard/`
