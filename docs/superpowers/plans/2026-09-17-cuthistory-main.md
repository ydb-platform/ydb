# CutHistory-only implementation on main

**Goal:** Implement the accepted small CutHistory boot scan from scratch on current upstream main, without any unmerged MoveData dependency.

**Spec:** `docs/superpowers/specs/2026-09-17-cuthistory-main.md`

## Global constraints

- Base `f6542ac7f6773a9bfc3b3a82aea71dc904ca1041`; branch `codex/cuthistory-main`; worktree `/home/kkhamitov/git/ydb/.claude/worktrees/codex-cuthistory-main`.
- Medium-effort implementation subagent. One worker owns source/tests; controller owns design and review.
- CutHistory only. No MoveData, executor vacuum, actualizer changes, MoveData flags/subscriptions/counters or ut_movedata dependency; no cherry-pick chain from previous PRs.
- One finite boot scan, NonEmpty bits, existing cache/broker, bounded batches, current interval GC/barrier/shared gates; no persisted proof or maintained reference counts.
- Existing `EnableCutHistory` flag. Share main's metadata request path with the smallest extraction; add only required missing readiness queries.
- Sensors for sends and durations; latest64 sent requests in EvHttpInfo; no new persisted journal.
- Comments only non-trivial and one line each; economical tests for distinct real behavior, no duplicate matrix.
- No edits or commits during active compilation. Use relwithdebinfo, no -j/force rebuild. No push/merge, no other worktree edits, no nested agents.

### Task 1: Implement, test and self-review standalone CutHistory

**Read first:** the authoritative spec. Prior wrong-base commit `2d0916d3b4b6ccdd8191fb9fd786eca5d8ddbf32` is only a reference for the algorithm and previously discovered loader/GC/fixture pitfalls, not a base or patch chain.

**Files:** compact `columnshard_cut_history.cpp`; existing ColumnShard lifecycle/private events/metadata loader, counters and monitoring; minimal BlobStorage readiness and sharing-admission integration. Add a small standalone `ut_cut_history` target using main's test_helper, or extend a suitable main test target if it avoids duplicated setup. No MoveData fixture or implementation may be imported.

- [ ] Read AGENTS.md and inventory current main APIs. Record clean base/HEAD and identify required adaptations before editing production code.
- [ ] Add a minimal real regression that fails because main does not issue ColumnShard cuts for an empty closed data interval; compile/run it RED. Reuse existing main runtime helpers; no fake reference counts/cost injection.
- [ ] Implement one finite `(PathId, PortionId)` snapshot after boot, committed/inserted/removed portions, per-entity DEFAULT own-blob marking, bounded one cache/broker request in flight, and fail-closed incomplete results.
- [ ] Implement exact interval/live keep/delete/delayed/shared/in-flight GC/covering persisted barrier gates; retry only these before ordinary background GC starts. Do not impose a fresh GC every boot when the interval is already covered.
- [ ] Extract only necessary existing metadata request submission from SetupMetadata. Preserve sorted cache-miss iteration, full-prefix index retry and Complete publication ordering.
- [ ] Cover pending source/destination and standalone links admissions with a monotonic invalidation flag; abandon the remaining proof when sharing starts. Check live shared/borrowed own-blob references. Do not block unrelated sharing after cuts. Ownership-moving sharing compatibility is explicitly outside this patch; do not claim the admission flag solves historical ownership transfer.
- [ ] Add send and duration sensors, bounded escaped EvHttpInfo records; keep source comments minimal.
- [ ] Economical focused tests cover live/uncommitted/default-index pins, pending GC then progress without rescan, persisted barrier reuse, no-history skip, cache batching/foreground progress, sensors and monitoring. Prefer extending compatible cases; use no MoveData calls. Cover an actual late sharing admission within an existing held-scan/GC case if straightforward, rather than another broad fixture.
- [ ] Run focused tests and full affected target once, then required style checks. Fix concrete failures; do not repeat unrelated tests after checks pass. Record command/results and retain evidence.
- [ ] Self-review simplicity/comments/coverage and main-only ancestry/diff. Report exact production/test delta, known performance limits, and commit only verified isolated CutHistory changes plus parent docs. Keep worktree for independent review.
