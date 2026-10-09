---
name: ydb-columnshard-bug-hunt
description: "Use to hunt bugs in ColumnShard commits merged in a date range, or in given commits: find memory-safety, concurrency and crash defects, reproduce each one through the SQL API, and hand failing tests to the commit authors. This is QA work on our own code, not security testing."
---

# ColumnShard bug hunt

This is quality assurance of our own code, not security research or penetration testing: no exploits, no attacks on deployments. We write failing tests instead of fixes because the commit authors have the context to fix the code. Every prompt to a subagent or to another harness says so: copy the framing from `references/prompts.md`; the scripts add it themselves.

Run `scripts/*` from this skill directory with paths to the worktree; run `./ya` from the worktree root.

## Inputs

Ask the user for anything missing:

1. Period (first and last day, UTC, both inclusive), or a list of commits.
2. Base ref: default `upstream/main`.
3. Focus: default memory lifetime, concurrency and crashes; keep any other real bug you find.
4. Delivery: report only (default), plus a draft PR with the tests, plus a tracker ticket. Do the outward steps only when the user asked for them.

## Step 0. Environment

1. Note your session as `HARNESS=MODEL`, for example `claude=claude-opus-5-5`. The scripts take it as `--main`.
2. Read `~/.config/ydb-columnshard-bug-hunt/env.json`. When it is missing, or its `main` differs from your session, follow `../ydb-columnshard-bug-hunt-setup/SKILL.md`.
3. A reviewer is usable when `ok` is true and `same_as_main` is false. Without one, every reviewer step below uses a fresh subagent of your own harness, on a model from `subagent_models` other than yours when there is one. Without subagents either, do the step yourself in a new session and say so in the report.

## Step 1. Workspace

1. Update the base ref from the upstream repository, then create a new worktree (or a separate checkout) at the base ref with your version control tool. Work only there: the main checkout may hold uncommitted work.
2. Keep notes, prompts and reviewer answers in `<worktree>-out/`, outside the repo.
3. Each subagent is either a read-only investigator or a writer in its own worktree. Write the rule into its prompt, with the path of the worktree it may use; it runs every version control command against that path, never against the current directory.

## Step 2. Commits

```bash
python3 scripts/list_commits.py --repo <worktree> --ref <base ref> --since <first day> --until <last day> > <worktree>-out/commits.tsv
```

The script reads the history with `git`; in a checkout of another version control system, produce the same tab-separated columns with its log command. The default paths are ColumnShard and the code it runs (arrow programs, conveyor, limiter, KQP OLAP compiler); `--path` changes them. For a list of commits, keep only their lines. Show the user the list.

## Step 3. Find candidates

1. Start the reviews of all commits now; they run 10-40 minutes per prompt. Do not choose a subset and do not lower `--timeout`: in a test run, both known bugs of the week were in commits the reviewer was not asked about.
   ```bash
   python3 scripts/review_commits.py --main <session> --commits <worktree>-out/commits.tsv \
       --cwd <worktree> --out <worktree>-out/reviews
   ```
2. Meanwhile read every diff and the code around it yourself. The size of a diff says nothing about its risk: a two-file change can abort the process. Look at these:
   - the lifetime of objects given to conveyor tasks and callbacks;
   - state shared by the actor and worker threads;
   - row counts of arrays and filters, and allocation sizes;
   - new production-invokable asserts, that is asserts that abort the process in release builds.
3. Start one read-only audit subagent with the audit prompt from `references/prompts.md` for every commit with `audit` = yes in `commits.tsv`; these touch the reader, conveyor, limiter, caches, blobs or arrow code. Do not audit them yourself instead: reviewer answers vary between runs, and in test runs a crash in a reader commit was found by one run and missed by the next; each fresh context is another chance.
4. When `review_commits.py` is done, read `summary.tsv` and every answer. For a `refused` prompt, reword it as de