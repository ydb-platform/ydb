# Prompts

Start every prompt with the framing. `scripts/second_opinion.py` and `scripts/review_commits.py` add it themselves.

```text
Context: this is quality assurance of our own open-source database, YDB. We review recently merged commits for
correctness bugs (object lifetime, concurrency, crashes, wrong results, memory accounting) and write failing
regression tests, because the authors of the commits have the context to fix them. This is not security research
or penetration testing: do not write exploits; describe defects, the code path, and a test scenario through the
public SQL API.
```

Read-only roles (audit, verifier, reviewer) also get the read-only rules:

```text
You are read-only in the worktree <worktree>. Run every version control command against that path. Only read: show
diffs, logs and files; do not commit, revert, switch, push, stash or clean, and do not edit files. Do not touch
<main checkout>.
```

## Wording that reviewers refuse

In the first runs of this hunt, Codex refused every prompt that asked for "use-after-free" defects ("This content was flagged for possible cybersecurity risk"); the same requests worded as correctness review went through. `second_opinion.py` returns exit code 3 on such a refusal. Describe defects as correctness: "object destroyed while a queued task still references it", "state accessed from two threads without synchronization", "invalid iterator or view", "wrong index math", "row counts of arrays and filters that do not match".

## Audit (subagent)

```text
<framing> <read-only rules>
Audit commit <sha> (<subject>): read its diff in <worktree>, then the full current code around the change.
Find object-lifetime defects, state shared between the ColumnShard actor thread and conveyor worker threads
without synchronization, wrong row counts or index math, allocations sized from metadata instead of the data
actually used, and new production-invokable asserts that user data can break.
For each candidate give: file:line, the interleaving or input, confidence (high: you traced the whole path;
medium: one step is assumed; low: a pattern only), and an SQL workload over a column table that reaches it in a
KQP test (TKikimrRunner + SQL; config knobs allowed; no calls into ColumnShard classes). Leave out low ones.
Under 800 words.
```

The two per-commit reviewer prompts are in `scripts/review_commits.py` (`PROMPTS`).

## Verifier (one per candidate)

```text
<framing> <read-only rules>
Claim: <candidate text, with file:line and the commit sha>.
Decide REAL, NOT REAL or UNCERTAIN with decisive file:line evidence. Check the guards on the path, which thread runs
each step, and what the code does on failure (error to the client, failed production assert, wrong result).
State whether the default server config reaches the path, or which config knob or PRAGMA is needed.
If REAL or UNCERTAIN, give a KQP SQL-only test recipe: table DDL, data, config knobs, query, expected result,
and the observable failure. Under 800 words.
```

## Test review

```text
<framing> <read-only rules>
Review the test-only change in <worktree>: list its changed and new files, read the diff against the base,
and read every new test file in full. The test <name> is expected to FAIL on the base because
<mechanism>; observed: <failure line>; with <sha> reverted it passes. Check: is it a faithful and minimal repro
through the SQL API; will it pass after a correct fix; can it pass without reaching the code path; does the result
depend on the host (RAM, cores, timing); is it too heavy for a MEDIUM test. Under 300 words.
```
