# Project Agents.md Guide for AI Agents

This file provides comprehensive guidance for AI agents working with this codebase.

## More Information About the Project

The documentation is available at <https://ydb.tech/docs/en/>.

## Project Structure for AI Agents Navigation

Every directory is a project (a library or a program).

- `/ydb/core`: Core modules.
- `/ydb/library`: YDB libraries.
- `/ydb/docs`: YDB documentation.
- `/library`, `/util`: Common libraries; never change them unless the change is explicitly required.
- `/yql`: YQL (query layer); shared with YDB.
- `/contrib`: Third-party and restricted dependencies.

## Building and Testing Requirements for AI Agents

Building and testing are performed using the Ya utility (`ya-tool`), which is located in the root of the codebase: `/ya`.

To build a project, from the **repository root** run:

```bash
./ya make path/to/project
```

Alternatively, `cd path/to/project` then run `/ya make`.

### Running tests with ya make (recommended for C++ unit tests, gtest, py3tests)

Use the following commands from the **repository root** (where `/ya` lives).

**Note:** Test runs can take a long time — `ya make` may need to build the whole project or large parts of it if required by dependencies. Do not abort the run; let it complete.

**Test size:** Use `-tt` by default — runs only **small** and **medium** tests (faster). Use `-ttt` when you need **large** tests too; prefer `-ttt` only for a narrow list of targets or a single test, as it can run much longer.

**Tests marked `SIZE(LARGE)` in ya.make do not run in PR-check** (PR-check uses `-tt` = small+medium only). When verification **requires** a large or specific test that would cover the change, **suggest** either: (1) **the exact command** to run locally, e.g. `./ya make -ttt --build "relwithdebinfo" -F 'TestNameOrPattern' path/to/test/target`, or a short list of such commands; or (2) running the workflow **Add run tests table to PR** (`.github/workflows/add_run_tests_table.yml`) via `workflow_dispatch` with the PR number — it adds a run-tests table to the PR so important tests can be run. Prefer attaching the concrete command when suggesting a specific test.

**Default (no sanitizers):** `./ya make -tt --build "relwithdebinfo"`. Use when there is no suspicion of memory/address/thread bugs.

- **Single test or narrow scope** — when changes touch one test binary or a small area, filter by test name with `-F`:

  ```bash
  ./ya make -tt --build "relwithdebinfo" -F 'TestNameOrPattern' path/to/test/target
  ```

  Example:

  ```bash
  ./ya make -tt --build "relwithdebinfo" -F 'FulltextIndexBuildTestReboots::BaseCase[PipeResetsBucket1]' ydb/core/tx/schemeshard/ut_index_build_reboots
  ```

- **Broader scope** — when changes affect more code:

  ```bash
  ./ya make -tt --build "relwithdebinfo" -F ydb/
  ```

**With sanitizers** — when there are suspicions of address, memory, or thread-safety issues, add `--sanitize="address"`, `--sanitize="memory"`, or `--sanitize="thread"` (use one at a time as needed). Example:

```bash
./ya make -tt --build "relwithdebinfo" --sanitize="address" -F 'TestNameOrPattern' path/to/test/target
```

Choose the single-test form for one test or one binary; use `-F ydb/` when the change touches multiple modules or you need broader coverage.

More information is available at <https://ydb.tech/docs/en/contributor/build-ya>.

## YDB context: fault tolerance, testing, and workloads

YDB is a **complex distributed system** with strict correctness and **fault-tolerance** requirements. When implementing functional changes, searching for root cause of failures, or debugging test failures, be **extra careful** and consider the following.

### Test and load contexts

- **Integration/functional tests** — `ydb/tests/`: `functional`, `stability`, `stress`, `olap`, `sql`, `library`, `compatibility`, `datashard`, `fq`, etc. Run via `./ya make -tt ... ydb/tests/functional` (or the relevant suite).
- **Nemesis** — fault-injection and load tests under failure conditions; results feed into analytics (e.g. `nemesis/aggregated_mart`). When changes touch replication, node failure, or cluster behavior, consider suggesting nemesis-related runs or stability/stress suites.
- **OLAP** — analytical workloads and benchmarks; tests under `ydb/tests/olap` and performance results (e.g. ClickBench, TPC-H). When touching query execution, storage, or analytics, suggest OLAP tests or workloads.
- **Workload (ydb CLI)** — load tests via `ydb workload` subcommands: **stock**, **key-value** (kv), **ClickBench**, **TPC-C**, **TPC-H**, **TPC-DS**, **topic**, **transfer**, **query**. Implementations live in `ydb/library/workload/`. Docs: `ydb/docs/.../workload` (e.g. reference in `ydb-cli/commands/workload`). When changes affect transactions, storage, or throughput, **suggest running the relevant workload** (e.g. `ydb workload stock ...` or the suite that matches the changed area).

### What agents should do

- **Functional changes:** Before finishing, consider impact on fault tolerance and data consistency; **suggest** running the most relevant test suite (e.g. `ydb/tests/stability`, `ydb/tests/stress`, `ydb/tests/olap`) or workload (`ydb workload <type>`).
- **Root cause / test failure:** When analyzing a failing test or bug, keep in mind nemesis, OLAP, and workload contexts; if the failure might be environment or load-related, suggest running stability/stress or a specific workload to reproduce.
- **Documentation:** Workload command reference is under `ydb/docs/.../ydb-cli/commands/workload`; link to it when suggesting load tests.

### Muted tests (`.github/config/muted_ya.txt`)

Some tests are **disabled** (in muted_ya.txt) because of a broken product, broken test, or broken infra. When reviewing code, investigating a failure, or making changes: **if you suspect that a muted test could be hiding a real bug** (e.g. the change touches the same component or code path that the muted test covers), **say so explicitly** and suggest checking or running that test (e.g. locally or after unmuting) to confirm there is no regression.

## Coding Conventions for AI agents

### C++ Standards for AI Agents

- Use modern C++ (no later than C++20).

### Documentation Guidelines for AI Agents

- Follow the style guide: <https://ydb.tech/docs/en/contributor/documentation/style-guide>
- Keep the structure consistent: <https://ydb.tech/docs/en/contributor/documentation/structure>

## Pull Request Guidelines for AI Agents

- Pull request descriptions must include a changelog entry (a short summary of the changes), followed by a detailed description for reviewers.
- Specify exactly one of the following PR categories:
  - New Feature
  - Experimental Feature
  - Improvement
  - Performance Improvement
  - User Interface
  - Bugfix
  - Backward-Incompatible Change
  - Documentation (changelog entry is not required)
  - Not for Changelog (changelog entry is not required)

For changes under **`.github/`** (workflows, scripts, config) and for **how to find test targets** (e.g. `ut_*` directories, integration tests under `ydb/tests/`), see [.github/copilot-instructions.md](.github/copilot-instructions.md).
