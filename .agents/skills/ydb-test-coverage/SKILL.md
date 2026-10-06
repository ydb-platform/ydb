---
name: ydb-test-coverage
description: Collect and interpret C++ source coverage from YDB tests using ya clang coverage on a remote Linux host through ssh_ya.
---

# YDB test coverage

Use this workflow for C++ source coverage. For Python, inspect the installed ya help for its Python coverage mode before constructing a command. For test design without a coverage measurement, follow the component's test conventions instead.

Root placement: coverage tasks can start in any component. A nested skill would hide the shared workflow; unrelated sessions need only its description.

## Select tests and scope

1. Read the nearest AGENTS.md and `ydb/agents/TESTS.md`. Identify the production files or changed lines to measure, the smallest relevant test targets, and any case filter. Include tests outside the component when they exercise its contract.
2. Use the installed `ssh_ya` from the local checkout. Inspect its help and select the remote Linux host. Global options precede the subcommand. Remote commands with shell syntax need explicit `sh -c` or `bash -c`; a single quoted command string alone is not sufficient.
3. Inspect `./ya make -hh` or `-hhh` on that host without syncing: `ssh_ya execute --skip-sync sh -c './ya make -hh'`. If the checkout lacks ya, sync the authorized branch first and retry the help command. Confirm coverage and output flags for that version before running tests.
4. Keep three scopes distinct: test targets and `-F` select executions; `-DCOVERAGE_TARGET_REGEXP` selects module paths to instrument; `--coverage-prefix-filter` and `--coverage-exclude-regexp` restrict reported source files. `build/plugins/coverage.py` uses `re.match` for module selection, so use an anchored expression with a path boundary rather than an unbounded substring.
5. Choose a unique absolute remote output directory outside the synced checkout. Record local commit and dirty diff, remote revision, host, build mode, targets, case filters, instrumentation regexp, and report filters. Exclude test code only when the intended denominator is production code; state the exclusions.

## Collect

Tests include the instrumented build. Use ssh_ya for the whole operation and follow root AGENTS.md build constraints. Do not edit sources during compilation. Use relwithdebinfo by default; repository coverage CI uses profile, so reproduce that mode when comparing with those CI results.

This is a template. Replace every placeholder and quote its value for both shells. Verify the full command with `ssh_ya --dry-run execute ...` first.

```bash
ssh_ya execute bash -o pipefail -c 'mkdir -p <out> && ./ya make --build relwithdebinfo -tA <target> -F "<case-filter>" --clang-coverage --coverage-report "-DCOVERAGE_TARGET_REGEXP=<module-regexp>" --coverage-prefix-filter="<source-prefix>" --output <out> 2>&1 | tee <out>/coverage-run.log | tail'
```

1. Omit `-F` when measuring the whole selected suite. A narrowly filtered run measures only those cases; label it as partial coverage. Check the actual executed cases: an unmatched filter may return success with no tests.
2. Keep the full log with tee and retain pipefail so tail cannot hide a failing build or test. Do not treat a partial report from a failed run as successful coverage.
3. Confirm report generation in the log and inspect the produced output paths. Existing CI expects `<out>/coverage.report/coverage.profdata` and `<out>/build_clang_coverage_report.log`; do not assume an HTML index or filename across ya versions.
4. Preserve the report, merged profile, instrumented binaries, matching sources, and logs together. Retain temporary outputs only when needed for export or investigation, using flags confirmed by help. Do not combine profiles from different binaries or revisions.

## Interpret and compare

1. Read per-file covered and total counts, then inspect uncovered lines and branches around the changed contract. Distinguish line, region, function, and branch metrics; report only metrics that the artifact actually contains.
2. Verify that intended production files appear in the report. A file missing because it was not instrumented is not evidence of full coverage. Treat executable code omitted from the denominator as a scope limitation.
3. For uncovered paths, identify concrete missing scenarios and appropriate test targets: errors, defaults, recovery, or boundary behavior. Assertions must check observable behavior; executing a line alone does not prove correctness.
4. Compare base and proposed changes only with the same targets, filters, build mode, report scope, and host configuration. Keep each revision's output separately. For changed-line coverage, map the diff to covered executable lines and state how non-executable lines were excluded; do not substitute whole-file percentage.
5. Report counts with percentages, revision, tested cases, scope, exclusions, failed or skipped tests, and artifact locations. Avoid setting a coverage threshold unless the task or component specifies one. Retrieve requested artifacts using available SSH file-transfer tools.

## Existing coverage pipeline

- `.github/actions/test_cpp_sdk_with_coverage/action.yaml`: C++ SDK coverage with instrumentation and source filters.
- `.github/actions/run_clang_codecov/action.yaml`: suite coverage with an output directory and retained temporary files.
- `.github/scripts/codecov/export_coverage_lcov.py`: LCOV and HTML export for registered suites only. Run its `--help`; `.github/scripts/codecov/codecov_suites.py` defines supported suites and source ownership. Do not invent a suite for an arbitrary component or modify that registry merely to collect local coverage.
- `build/plugins/coverage.py`: module instrumentation filters and explicit coverage enable/disable overrides. Report source filters are separate from those module filters.
