---
name: ydb-columnshard-bug-hunt
description: "Use to hunt bugs in ColumnShard commits merged in a date range, or in given commits: find memory-safety, concurrency and crash defects, reproduce each one through the SQL API, and hand failing tests to the commit authors. This is QA work on our own code, not security testing."
---

# ColumnShard bug hunt

This is quality assurance of our own code, not security research or penetration testing: no exploits, no attacks on deployments. We write failing tests instead of fixes because the commit authors have the context to fix the code. Every prompt to a subagent or to another harness says so: copy the framing from [references/prompts.md](references/prompts.md); the scripts add it themselves.

Run `scripts/*` from this skill directory with paths to the worktree; run `./ya` from the worktree root.

## Inputs

Ask the user for anything missing:

1. Period (first and last day, UTC, both inclusive), or a list of commits.
2. Base ref: default `upstream/main`.
3. Focus: default memory lifetime, concurrency and crashes; keep any other real bug you find.
4. Delivery: report only (default), plus a draft PR with the tests, plus a tracker ticket. Do the outward steps only when the user asked for them.

## Step 0. Environment

1. Note your session as `HARNESS=MODEL`, for example `claude=claude-opus-5-5`. The scripts take it as `--main`.
2. Read `~/.config/ydb-columnshard-bug-hunt/env.json`. When it is missing, or its `main` differs from your session, follow the [setup skill](../ydb-columnshard-bug-hunt-setup/SKILL.md).
3. A reviewer is usable when `ok` is true and `same_as_main` is false. Without one, every reviewer step below uses a fresh subagent of your own harness, on a model from `subagent_models` other than yours when there is one. Without subagents either, do the step yourself in a new session and say so in the report.

## Step 1. Workspace

1. Update the base ref from the upstream repository, then create a new worktree (or a separate checkout) at the base ref with your version control tool. Work only there: the main checkout may hold uncommitted work.
2. Keep notes, prompts and reviewer answers in `<worktree>-out/`, outside the repo.
3. Each subagent is either a read-only investigator or a writer in its own worktree. Write the rule into its prompt, with the path of the worktree it may use; it runs every version control command against that path, never against the current directory.

## Step 2. Commits

```bash
python3 scripts/list_commits.py --repo <worktree> --ref <base ref> --since <first day> --until <last day> > <worktree>-out/commits.tsv
```

[scripts/list_commits.py](scripts/list_commits.py) reads the history with `git`; in a checkout of another version control system, produce the same tab-separated columns with its log command. The default paths are ColumnShard and the code it runs (arrow programs, conveyor, limiter, KQP OLAP compiler); `--path` changes them. For a list of commits, keep only their lines. Show the user the list.

## Step 3. Find candidates

1. Start the reviews of all commits with [scripts/review_commits.py](scripts/review_commits.py) now; they run 10-40 minutes per prompt. Do not choose a subset and do not lower `--timeout`: in a test run, both known bugs of the week were in commits the reviewer was not asked about.
   ```bash
   python3 scripts/review_commits.py --main <session> --commits <worktree>-out/commits.tsv \
       --cwd <worktree> --out <worktree>-out/reviews
   ```
2. Meanwhile read every diff and the code around it yourself. The size of a diff says nothing about its risk: a two-file change can abort the process. Look at these:
   - the lifetime of objects given to conveyor tasks and callbacks;
   - state shared by the actor and worker threads;
   - row counts of arrays and filters, and allocation sizes;
   - new production-invokable asserts, that is asserts that abort the process in release builds.
3. Start one read-only audit subagent with the audit prompt from [references/prompts.md](references/prompts.md) for every commit with `audit` = yes in `commits.tsv`; these touch the reader, conveyor, limiter, caches, blobs or arrow code. Do not audit them yourself instead: reviewer answers vary between runs, and in test runs a crash in a reader commit was found by one run and missed by the next; each fresh context is another chance.
4. When the reviews finish, read `summary.tsv` and every answer. If a reviewer refuses the QA request, explain the suspected bug in plain language and retry once; see [references/prompts.md](references/prompts.md) for examples. If the review still cannot run, note the missing review and use the fallback from step 0.
5. Combine your findings and the reviewer answers into a candidate list. Record the suspected commit, mechanism, file:line evidence and an SQL trigger for each candidate; merge duplicate claims about the same defect.

## Step 4. Check candidates before writing tests

Have a fresh reviewer check each claim for false positives before spending time on a SQL reproducer. An existing guard, an object's lifetime or an unreachable code path may rule out the suspected bug.

1. Give each candidate to a fresh read-only reviewer using the verifier prompt in [references/prompts.md](references/prompts.md). Use [scripts/second_opinion.py](scripts/second_opinion.py) with the reviewer from step 0, or its subagent fallback.
2. Ask the reviewer to trace the failure through the code, check what might prevent it, and explain how SQL could trigger it. Record `REAL`, `NOT REAL` or `UNCERTAIN`, the supporting file:line evidence and any required config knobs or PRAGMAs.
3. Save rejected claims with the reasons they were rejected. Take the remaining candidates and their SQL test recipes to the next step. A reviewer verdict is provisional until a test reproduces the failure.

## Step 5. Reproduce and review tests

1. Follow [references/repro.md](references/repro.md) to write SQL-level KQP tests in `ydb/core/kqp/ut/olap/`. Respect its allowed APIs and config knobs; write tests, not production fixes.
2. Confirm the predicted failure on the base and a passing result in a separate worktree with the suspected commit reverted. Check that the test reaches the intended path and data shape, and save commands, failure output and the revert result. Record unsuccessful attempts and stress-run counts for candidates that remain unproven.
3. Have the reviewer from step 0 review the test-only diff with the test review prompt in [references/prompts.md](references/prompts.md). Address findings and rerun affected tests before delivery. Only reliable failing reproductions belong in the test PR.

## Step 6. Search existing reports and deliver

1. Follow [references/deliver.md](references/deliver.md) to search GitHub issues and the Tracker queues chosen by the user, including open and closed reports. Ask which queues to search when they have not been specified. Compare the mechanism, affected code and introducing commit before calling a result a duplicate; record links and any unavailable searches.
2. Write the report with reproduced, unproven, rejected and out-of-scope findings, reproduction evidence and existing reports. Prepare a tests-only change containing only reliable reproductions.
3. When requested, publish the tests as a draft PR and create the period's Tracker ticket following the delivery reference. Include the reproduction evidence, introducing commits, authors, report and test PR links; link only reports about the same defect. Report-only runs stop at local artifacts.
4. Give the user the report path or link, test results, existing issue links, any created PR and ticket links, and remaining gaps.

## Maintenance

[scripts/process_runner.py](scripts/process_runner.py) is the shared helper imported by the reviewer and setup scripts. It runs commands and cleans up POSIX process groups on timeout; it is not a standalone command.

Regression tests:

- [test_list_commits.py](scripts/tests/test_list_commits.py): date filtering, rename classification and commit metadata.
- [test_review_commits.py](scripts/tests/test_review_commits.py): commit selection, prompt coverage and session checks.
- [test_second_opinion.py](scripts/tests/test_second_opinion.py): reviewer selection, response handling and output files.
- [test_runner_timeouts.py](scripts/tests/test_runner_timeouts.py): output handling and child-process cleanup in both reviewer and setup runs.

Run them from this skill directory with `python3 -m unittest discover -s scripts/tests -v`. [evals/evals.json](evals/evals.json) contains end-to-end bug-hunt scenarios for evaluating changes to the workflow.
