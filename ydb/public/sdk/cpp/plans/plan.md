# Plan: MessageGroupId fairness selection for STD queues in `TStorage::Next`

## Goal

Bring the STD queue read path (`KeepMessageOrder == false`) to parity with the FIFO
fairness behaviour, while **reusing** the existing data structures. Concretely:

- STD `Next` should be able to select the message whose `MessageGroupId` has *not*
  been read for the longest time (fairness), instead of only scanning by increasing
  `Offset`.
- Unlike FIFO, STD may return **any number** of messages with the same
  `MessageGroupId` simultaneously (no "one in-flight per group" restriction).
- Add a parameter to `Next` selecting between:
  - `ByOffset` — return the oldest message (smallest offset), i.e. current STD behaviour.
  - `ByMessageGroupFairness` — pick based on least-recently-served MessageGroupId with fairness.
- Default policy for STD reads is `ByMessageGroupFairness`.
- There is no mixed / every-Nth mode. A call uses whichever policy the caller passes.
- **FIFO grouped selection stays as it is.** Iteration order of `UnorderedOffsets` is not
  part of that requirement: the container is a hash set, so the order is arbitrary.
  Tests must not canonicalize it and must not depend on numeric hash values — only on
  hash equality.

## Decisions

These override earlier wording in this plan where they disagree.

### Selection parameter

`Next` / `Read` take `EReadSelectionPolicy`:

- `ByOffset` — smallest eligible offset. Resume through `TStorage::TPosition`, the same cursor the offset scan on trunk uses (`SlowPosition`, then `FastPosition`). Do not keep `UnorderedOffsets` ordered for this. `std::set` / `flat_hash_set` iteration is not an offset order.
- `ByMessageGroupFairness` — the group fairness walk.

No third mode, no internal counter, no random fraction of calls. If a caller wants an oldest message, it passes `ByOffset` on that call. The default for STD remains `ByMessageGroupFairness`. FIFO ignores the parameter.

### Where a not-yet-read group is inserted

Unspecified. Pushing it to the back, as FIFO does, is allowed. It is not a requirement, and neither is pushing it to the front. Tests must accept any insertion strategy: they must not assert which never-read group comes first. The required fairness property is the one FIFO already has for a group that **has** been read: after `Next` serves it, that group goes to the back and is not served again while another eligible group is still ahead of it.

### Groupless messages

Messages with `HasMessageGroupId == false` take part in the same `Next` selection as grouped ones.

Both of the following are valid:

- Prefer groupless on some calls and grouped messages on others (for example even calls try groupless first, odd calls try a group first). Either class can be returned while the other class still has `Unprocessed` messages.
- Walk the fairness list first and only then `UnorderedOffsets`, which is what FIFO does today.

The implementation uses the second strategy so STD and FIFO share `SearchForEligibleMessage` and the existing groupless fallback. Consequences of that choice:

- While any eligible group remains, `ByMessageGroupFairness` returns a grouped message.
- A groupless message is returned once no eligible group remains, including when grouped messages exist but are locked, skipped, delayed, or retention-expired.
- A grouped message can be returned while groupless `Unprocessed` messages exist.

`UnorderedOffsets` stays `absl::flat_hash_set<ui64>`. Order inside it is arbitrary and is not restored as a sorted order. Tests assert the set of returned groupless offsets, not their sequence.

### Unlock and the fairness list

On explicit `Unlock`, moving the group to the back is allowed no matter how many `Unprocessed` messages it still has. Tests must not require the group to keep its previous position, and must not require that it moves. After unlock the message is `Unprocessed` again and is readable on a later `Next`.

### What tests must not depend on

- Numeric values of hashes, or any total order of hashes. Two messages are in the same group exactly when their hashes compare equal.
- Iteration order of `UnorderedOffsets`.
- The slot where a group that has not been read yet is inserted.
- The list position of a group after `Unlock`.

## Current State (analysis)

Files involved:
- [`mlp_storage.h`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.h:1)
- [`mlp_storage.cpp`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:1)
- [`mlp_storage__serialization.cpp`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage__serialization.cpp:1)
- [`mlp_consumer.cpp`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_consumer.cpp:1)
- [`mlp.h`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp.h:1)
- Tests: [`ut/mlp_storage_ut.cpp`](../ydb/core/persqueue/pqtablet/partition/mlp/ut/mlp_storage_ut.cpp:1)

### FIFO selection machinery (to be reused)
- `TMessage::NextMessageGroupIdOffset_` links messages of the same group into a
  per-group singly-linked chain (see [`mlp_storage.h`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.h:114)).
- `TMessageGroups` holds:
  - `Groups` (`hash<groupIdHash, TSingleMessageGroupIdInfo>`) with `FirstOffset`,
    `LastOffset`, `Size`, `Locked`.
  - `UnlockedMessageGroupsId` (set) + `UnlockedMessageGroupsIdViewOrder`
    (`TIntrusiveList<TOrderedMessageGroupIdHash>`) — the fairness queue.
  - `UnorderedOffsets` (`absl::flat_hash_set<ui64>`) — groupless offsets. Iteration order is arbitrary.
- [`SearchForEligibleMessage()`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:204)
  walks `UnlockedMessageGroupsIdViewOrder`, for each group starts at `FirstOffset` and
  walks the chain, returns the first usable message and the iterator so `Next` can
  **rotate** consumed groups to the back of the fairness list.
- [`Next()`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:239): FIFO branch
  calls `SearchForEligibleMessage`, rotates, locks, then falls back to
  `UnorderedOffsets`. STD branch scans SlowMessages then Messages by offset.
- Group state maintenance functions all early-return when `!KeepMessageOrder`:
  [`UpdateMessageGroupForNewMessage`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:910),
  [`UpdateMessageGroupOnMessageStatusChange`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:853),
  [`UpdateMessageGroupForRemovedMessage`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:883),
  and [`BuildAndLinkMessageGroups`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage__serialization.cpp:438).

### Key semantic difference FIFO vs STD
- **FIFO**: locking the group head marks the whole group Locked/removed from the
  unlocked set; the head only advances (to `NextMessageGroupIdOffset`) on
  commit/removal. => at most one in-flight message per group; strict order.
- **STD (target)**: a group stays *unlocked/eligible* as long as it has **any**
  `Unprocessed` message. Locking one message must **not** block the group; instead the
  group's "read cursor" advances to the next Unprocessed message, and the group is
  rotated to the back of the fairness list. The parent-partition locking / external
  blacklist logic does **not** apply to STD (`KeepMessageOrder == false` short-circuits
  `CanReadMessageGroupIdHash*` to `true`).

## Design

### 1. Selection policy parameter
Add an enum (in `mlp_storage.h`, `NKikimr::NPQ::NMLP`):
```
enum class EReadSelectionPolicy {
    ByOffset,               // oldest offset first (legacy STD scan)
    ByMessageGroupFairness, // least-recently-served MessageGroupId, with fairness
};
```
- Add parameter to `Next(..., EReadSelectionPolicy policy)` and
  `Read(..., EReadSelectionPolicy policy)`.
- Policy only affects the STD branch. In the FIFO branch the policy is ignored
  (FIFO is always group-based and ordered) — keeps FIFO unchanged.
- Default value: `EReadSelectionPolicy::ByMessageGroupFairness` (see step 8). FIFO
  callers are unaffected since FIFO ignores the policy.

### 2. Always maintain group structures for STD
- Remove the `!KeepMessageOrder` early-returns from the four maintenance functions,
  replacing them with **mode-aware** logic (see step 3). This means STD builds and
  keeps `Groups`, the per-group chains, `UnlockedMessageGroupsId(+ViewOrder)` and
  `UnorderedOffsets` just like FIFO.
- `BuildAndLinkMessageGroups` must run for STD too (drop its `!KeepMessageOrder`
  guard) so restored snapshots/WAL rebuild the structures.
- Accept the extra memory/CPU for STD unconditionally (explicit decision).

### 3. Mode-aware group semantics
Introduce a single predicate, e.g. `bool GroupIsEligible(const TSingleMessageGroupIdInfo&)`
and a helper to advance a group's read cursor. Behaviour split:

- FIFO (unchanged): group considered locked when its head message is
  Locked/Delayed/DLQ/parent-locked; head advances only on commit/removal.
- STD: group eligible iff it still has at least one `Unprocessed` message.
  - `TSingleMessageGroupIdInfo` gains a notion of "next unprocessed offset"
    (the read cursor). Options:
    - Reuse `FirstOffset` as "first non-committed offset" and add a separate
      `ReadCursorOffset` (first `Unprocessed`) used by fairness selection; OR
    - Walk the chain from `FirstOffset` skipping non-`Unprocessed` messages inside the
      generalized search (simpler, no new field, slightly more work per read).
  - Preferred: keep the chain-walk approach in the generalized search (step 4) so no
    new persisted field is required. Track an in-memory `UnprocessedCount` (or derive
    eligibility from `Size` minus locked/committed) to know when to drop the group from
    the unlocked set.
  - On **lock** (STD): decrement the group's unprocessed count; if it becomes 0 remove
    from `UnlockedMessageGroupsId(+ViewOrder)`, else rotate to back (fairness). Group is
    NOT added to `LockedMessageGroupsId` (STD has no group-level lock concept).
  - On **unlock/undelay/DLQ-wakeup** (STD): increment unprocessed count; ensure group is
    present in `UnlockedMessageGroupsId(+ViewOrder)`. Moving the group to the back on
    unlock is allowed regardless of the remaining unprocessed count, and so is leaving
    it where it was. Tests must not pick one of these.
  - On **commit/remove** (STD): decrement `Size`; when `Size == 0` erase the group.

### 4. Generalized eligible-message search
Refactor [`SearchForEligibleMessage()`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_storage.cpp:204)
so the group chain walk selects the first message that is:
`Unprocessed` AND not retention-expired AND not in `skipMessageGroups`.
- For FIFO the head is always `Unprocessed` (guaranteed by existing invariants), so
  behaviour is identical.
- For STD the walk skips Locked/Delayed messages within the group to find the next
  `Unprocessed` one. Reuses `TryGetMessage` semantics but must tolerate non-`Unprocessed`
  messages in the chain (relax the `AFL_ENSURE(status == Unprocessed)` for the STD walk,
  or add a variant that treats non-Unprocessed as "skip and continue").
- Returns `{Message, Offset, OrderIterator}` exactly as today so `Next` can rotate.

### 5. `Next` branching
```
Next(deadline, position, skip, policy):
  if KeepMessageOrder:              # FIFO, unchanged
      ... existing FIFO body ...
  else:                             # STD
      if policy == ByMessageGroupFairness:
          # group-based path reusing SearchForEligibleMessage + rotate + UnorderedOffsets
      else: # ByOffset
          ... existing STD offset-scan body (unchanged) ...
```
- The fairness STD path mirrors the FIFO path: `SearchForEligibleMessage`, rotate the
  chosen group to the back of `UnlockedMessageGroupsIdViewOrder`, `DoLock`, then fall
  back to `UnorderedOffsets` (groupless). This is the selected groupless strategy;
  see Decisions. `ByOffset` does not use that set: it continues from `TPosition`.
- `DoLock` must call the mode-aware status-change so STD keeps the group eligible if
  more unprocessed messages remain (step 3).

### 6. Metrics & DebugString reconciliation
- `InflightMessageGroupCount` / `LockedMessageGroupCount` are currently FIFO-only
  (guarded by `KeepMessageOrder && HasMessageGroupId`). Decide STD semantics:
  - `InflightMessageGroupCount` = number of live groups (now also populated for STD).
  - `LockedMessageGroupCount`: STD has no group lock; keep it FIFO-only (skip increments
    for STD in `DoLock`/`RemoveMessage`/`DoCommit`).
- `TMessageIterator::operator*` sets `MessageGroupIsLocked` from
  `KeepMessageOrder && ... !UnlockedMessageGroupsIdContains(...)`. For STD, "group locked"
  is not meaningful — keep returning `false` for STD (leave the `KeepMessageOrder` guard).
- `InitMetrics` sets `InflightMessageGroupCount = KeepMessageOrder ? Groups.size() : 0`;
  update to `Groups.size()` unconditionally (since STD now builds Groups) OR keep 0 for
  STD if we prefer not to expose it — align with the metric decision above.

### 7. Consumer wiring (default policy)
- In [`mlp_consumer.cpp`](../ydb/core/persqueue/pqtablet/partition/mlp/mlp_consumer.cpp:823)
  the `Read` call passes the new policy. Default for STD consumers =
  `ByMessageGroupFairness`. Determine whether this should be configurable via consumer
  config; if not required now, hard-wire `ByMessageGroupFairness` for STD and let FIFO
  ignore it.
- `Read` forwards `policy` to each `Next` call.

### 8. Defaults (chosen: default = `ByMessageGroupFairness`)
- Give `Next`/`Read` a default argument of `EReadSelectionPolicy::ByMessageGroupFairness`
  so the new fairness behaviour is the out-of-the-box STD semantics and the consumer
  layer gets it without extra wiring.
- Consequence: existing STD unit tests that rely on strict increasing-offset return order
  must be updated to either pass `EReadSelectionPolicy::ByOffset` explicitly (when the
  test's intent is to verify offset scanning) or to assert against the fairness oracle.
  Enumerate and fix these call sites as part of step 9 (the ~90 `Next`/`Read` sites in
  [`ut/mlp_storage_ut.cpp`](../ydb/core/persqueue/pqtablet/partition/mlp/ut/mlp_storage_ut.cpp:1)
  — most are FIFO or single-message and unaffected; only STD multi-message ordered
  assertions need changes).
- FIFO ignores the policy entirely, so FIFO call sites need no changes.

### 9. Tests
- STD fairness tests must accept any initial insertion order of not-yet-read groups.
  With no messages added between calls, one pass over the currently eligible groups
  returns each group once, and the next pass repeats that same cyclic order. Inside a
  group the smaller `Unprocessed` offset comes first. Do not assert which group is first.
- Groupless tests follow the selected strategy (groups, then `UnorderedOffsets`): a
  grouped message is returned while an eligible group exists; groupless offsets come
  afterwards as a set, with no asserted order. Also drain both classes together.
- `ByOffset` keeps ascending order across a shared `TPosition`, including a locked hole,
  a message sitting in the slow zone, and a retention-expired offset.
- After snapshot + WAL, `Next` returns an `Unprocessed` message and does not return one
  that was locked before the snapshot.
- Retention: an expired head is skipped and a later message of that group is returned;
  a group whose messages are all expired does not hide another group.
- `skipMessageGroups` hides that group and does not hide the others.
- A delayed message is not returned. After the delay expires and deadlines are processed
  it can be returned. Do not assert where that group sits in the fairness list.
- DLQ: in STD a DLQ message does not block the rest of its group. `WakeUpDLQ` makes it
  readable again.
- Removing a non-head message of a group from the slow zone leaves the chain readable.
- Unlock makes that message readable again. Do not assert fairness-list position.
- Keep existing FIFO tests green. Do not add a FIFO test that pins `UnorderedOffsets`
  iteration order.

## Risks / Watch-points
- Relaxing `AFL_ENSURE(status == Unprocessed)` in the chain walk must not weaken FIFO
  invariant checks — gate the relaxation behind the STD path.
- Group cursor bookkeeping (unprocessed count) must stay consistent across
  lock/unlock/commit/undelay/DLQ-move/DLQ-wakeup/retention-remove for STD, or metrics /
  eligibility drift. This is the highest-risk area; cover with the randomized model test.
- `MessageGroupIsLocked`/metrics changes are observable by the actor layer — verify no
  consumer logic assumes these are zero for STD.
- Serialization format is unchanged (no new persisted field) — restore path just needs
  `BuildAndLinkMessageGroups` enabled for STD.

## Execution Order (todos)
1. Add `EReadSelectionPolicy` enum; thread `policy` through `Next`/`Read`
   (default `ByMessageGroupFairness`).
2. Enable group-structure maintenance for STD (drop `!KeepMessageOrder` early-returns +
   enable `BuildAndLinkMessageGroups`), introducing mode-aware branches.
3. Implement STD group semantics (many-in-flight, unprocessed-count eligibility, rotation).
4. Generalize `SearchForEligibleMessage` chain walk (skip non-Unprocessed for STD).
5. Add STD fairness branch in `Next`; keep FIFO + STD-ByOffset paths unchanged.
6. Reconcile metrics + `DebugString` + `TMessageIterator` + `InitMetrics` for STD.
7. Wire `ByMessageGroupFairness` default from consumer actor into `Read`.
8. Extend/duplicate `TFairnessModel` and add STD tests (fairness + ByOffset regression +
   restore).
9. Run full mlp_storage unit suite; ensure FIFO tests unchanged.
