# Repository instructions for GitHub Copilot

Instructions for working with the YDB codebase. **Apply these guidelines when writing code, reviewing PRs, or changing CI/config.** For project structure, building, running tests, coding conventions, and PR guidelines use [AGENTS.md](/AGENTS.md) in the repository root. This file adds only:

1. **How to find test targets** (below) — supplement to the tests section in AGENTS.md.
2. **Impact of changes under `.github/`** — workflows, scripts, config, actions.

Docs: <https://ydb.tech/docs/en/>.

---

## How to find test targets

- **C++ unit tests:** Look for directories named **`ut_*`** (e.g. `ut_index_build_reboots`) under the module. The target path is the directory path: e.g. `ydb/core/tx/schemeshard/ut_index_build_reboots`.
- **`ya.make`** in such directories usually contains **`UNITTEST_FOR(...)`** and **`SIZE(SMALL|MEDIUM|LARGE)`**; `-tt` runs small+medium, `-ttt` adds large. **Tests with `SIZE(LARGE)` do not run in PR-check.** When verification needs a large or specific test, suggest the **exact command** (e.g. `./ya make -ttt --build "relwithdebinfo" -F '...' path/to/test/target`) or the workflow [add_run_tests_table.yml](.github/workflows/add_run_tests_table.yml) (workflow_dispatch with PR number) to add a run-tests table to the PR.
- **Python / integration tests:** Under `ydb/tests/` (e.g. `ydb/tests/functional`, `ydb/tests/library`). Run via `./ya make -tt ...` with the appropriate test path.

**YDB fault tolerance and workloads:** YDB is a complex, fault-tolerant system. When making functional changes or analyzing test failures, consider **nemesis** (fault injection), **OLAP** tests (`ydb/tests/olap`), **stability/stress** (`ydb/tests/stability`, `ydb/tests/stress`), and **load via `ydb workload`** (stock, kv, clickbench, tpcc, tpch, tpcds, topic, transfer, query). Suggest running the relevant suite or `ydb workload <type>` when appropriate. Full guidance: [AGENTS.md](/AGENTS.md) section "YDB context: fault tolerance, testing, and workloads".

---

## Changes to `.github` (workflows, scripts, config, actions)

When you change anything under **`.github/`** (workflows, scripts, config files, or composite actions), **assess impact** and **warn** about consequences. Use two priority levels: **critical / real** (high impact, likely breakage) and **potential** (possible side effects, follow-up checks needed).

### 1. Map impact before editing

- **Workflows** (`.github/workflows/*.yml`, `*.yaml`): which jobs/steps call which scripts or actions; which use `secrets.*`, `vars.*`, or config paths.
- **Scripts** (`.github/scripts/**`): which workflows or actions invoke them; expected CLI args, env vars, and input files (e.g. `muted_ya.txt`).
- **Config** (`.github/config/*`): who reads it — e.g. `muted_ya.txt`, `stable_tests_branches.json`, `stable_nightly_branches.json` are used by multiple workflows and by the **test_ya** action (PR-check, postcommit, run_tests, update_muted_ya, create_issues_for_muted_tests).
- **Actions** (`.github/actions/**`): which workflows call them; inputs/outputs and any hardcoded paths (e.g. `.github/config/muted_ya.txt` in **test_ya**).

### 2. Critical / real problems (warn with high priority)

- **PR-check / gate / postcommit:** Changes to `pr_check.yml`, `gate_postcommits.yml`, `postcommit_*.yml`, or to **test_ya** / **build_and_test_ya** can break merge checks and postcommit runs. Any change to how `muted_ya.txt` is read or passed (path, format, YAML conversion in `mute_utils.py`) affects all PR and postcommit test runs.
- **Mute pipeline:** `update_muted_ya.yml` and `create_issues_for_muted_tests.yml` depend on: `create_new_muted_ya.py`, `get_muted_tests.py`, `flaky_tests_history.py`, `tests_monitor.py`, and on **`.github/config/muted_ya.txt`** path and format. Changing mute/unmute rules (in code or in `mute_rules.md`) can change which tests get muted and affect analytics and Telegram notifications.
- **Paths and interfaces:** Renaming or moving **`.github/config/muted_ya.txt`**, or changing the format expected by `mute_utils.py` / `transform_build_results.py` / `create_new_muted_ya.py`, will break workflows and the **test_ya** action unless all call sites are updated.
- **Muted tests and real bugs:** Tests in `muted_ya.txt` are disabled (broken product/test/infra). When reviewing code or investigating failures, if a **muted test could be hiding a real bug** (e.g. the change or fix touches the same area as that test), **warn explicitly** and suggest verifying (e.g. run the test locally or consider unmuting to confirm no regression).
- **Secrets and vars:** Workflows use `CI_YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS`, `YDBOT_TOKEN`, `GH_PERSONAL_ACCESS_TOKEN`, `TELEGRAM_YDBOT_TOKEN`, `TELEGRAM_MUTE_CHAT_ID`, `TEAM_TO_RESPONSIBLE_TG`, etc. Changing step logic that uses these can cause auth/notification failures.

### 3. Potential problems (warn, suggest verification)

- **Schedules and triggers:** Changing `on.schedule` or `on.workflow_dispatch` / `on.pull_request` can change how often jobs run or when they start; document the new behavior.
- **Reusable workflows:** `run_tests.yml` is called by regression_* and run_and_debug_tests; changing its inputs (e.g. `branches_config_path`, `test_targets`) affects all callers.
- **Analytics chain:** `collect_analytics.yml`, `collect_analytics_fast.yml`, `monitoring_queries.yml`, and `create_issues_for_muted_tests` run scripts in `.github/scripts/analytics/` and write to YDB; changing query paths, table names, or script order can break dashboards and downstream jobs.
- **Telegram / alerts:** `alert_queued_jobs.py`, `parse_and_send_team_issues.py`, and RAM/alert steps depend on Telegram secrets and vars; logic or env changes can break notifications.
- **Branch/config lists:** `update_muted_ya.yml` reads branches from `BRANCHES_CONFIG_PATH` (e.g. `stable_tests_branches.json`). Changing that path or the JSON shape can change which branches get mute-update PRs.

### 4. Backward compatibility when a component is used from another branch

If a **script**, **composite action**, or **reusable workflow** can be run from **another branch** (e.g. `stable-*`, `prestable-*`, or any workflow that runs on a ref other than `main`), the job checks out **that branch** — so it executes **that branch’s version** of the script/action, not the one on `main`.

**What this can lead to:**

- **Failures on release/other branches:** A breaking change on `main` (new required CLI argument, changed config path, changed output format) is not present on the old branch. When the workflow runs there, it still uses the old script/action. If only the **workflow YAML** was backported (e.g. new step or new input), the **code** on that branch is still old — the new args or paths are passed to the old script → **runtime errors** or wrong behavior.
- **Divergent behavior:** `main` has the new logic; stable/release branches keep the old one. The “same” workflow name behaves differently depending on branch — **hard to reason about** and **support**.
- **Silent breakage after partial backport:** Someone backports a workflow change but not the script; the run may fail in CI or produce incorrect results (e.g. wrong mute list, wrong artifact).

**What to do:** When changing a script or shared step that may be invoked from another branch (check which workflows run on which refs, e.g. `stable-*` in `on.branches` or matrix over branches), either **(1)** keep **backward compatibility** (optional args, same paths/formats, additive changes only), or **(2)** explicitly plan **backport** of the script/action to all branches that use it, and **warn** in the PR that release branches must be updated.

### 5. What to do in the response

- **Before applying changes:** Briefly list which workflows, actions, or scripts are affected and whether any **critical** path is touched (PR-check, gate, postcommit, mute pipeline, muted_ya path/format).
- **Warn explicitly:** e.g. “Critical: this changes how muted_ya.txt is read; PR-check and postcommit use it — ensure path/format stay compatible” or “Potential: schedule change; verify run frequency is still intended.”
- **Suggest checks:** If you changed a script or workflow, suggest running the workflow manually (workflow_dispatch) or checking dependent workflows’ docs (e.g. `.github/scripts/telegram/README.md`, `.github/config/mute_rules.md`).

