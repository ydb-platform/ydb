# PQRB: read balancing

The `PersQueueReadBalancer` tablet assigns topic partitions to a consumer's
read sessions. The rules below describe ordering across split/merge.
Implementation:
[`read_balancer__balancing.cpp`](read_balancer__balancing.cpp),
[`read_balancer__balancing.h`](read_balancer__balancing.h). Tests:
[`ut/balancing_ut.cpp`](ut/balancing_ut.cpp).

## Terms

- **Lock** — the partition is assigned to a session (`TEvLockPartition`);
  the session is reading from it. A locked partition may be active or inactive
  (for example a finished parent stays in the family together with its
  children).
- **Unlocked** — the partition is not assigned to any read session.
- **Active partition** — its messages have not been fully read yet.
- **Inactive partition** — a read session has consumed all of its messages,
  or it is **Commit**.
- **Finish** — a read session has reached the end of the partition. The
  signal comes from the read session.
- **Commit** — the committed offset equals EndOffset. Comes from the
  partition.
- **Read session** — one read connection (`TSession`, pipe) that partitions
  are balanced into.
- **Consumer** — read state on the balancer (`TConsumer`). Lives while at
  least one session with this consumer id exists.
- **Root partition** — a partition with no parent: it did not come from a
  split or merge of another partition, or the parent has already been
  deleted.
- **Parent partition** — the partition whose split or merge produced the
  current one.
- **Child partition** — a partition produced by a split or merge of the
  current one.

**Finish** and **Commit** exist only for a partition that has children
(new messages can no longer land in it). A partition without children cannot
get these events: `ReadingFinished` and `Commited` are always false. This
holds even without auto-partitioning: every partition is then a leaf.

A topic may have auto-partitioning on or off. The balancing algorithm does
not change, but with auto-partitioning off some of its branches are unused.

## Order

When the first read session connects, only root partitions are given out
for reading.

A child partition is given out for reading only after **all of its parents
have been processed**, except when a session listed that child explicitly
(see [Explicit partitions](#explicit-partitions)). If Finish carried
`ScaleAwareSDK`, **Finish** or **Commit** is enough. For the old SDK, Finish
alone is not enough: it also needs `StartedReadingFromEndOffset` (reading
started at the end) or **Commit**. Otherwise the children are not readable
and the delay heuristic kicks in (see below).

If a read session dies, **Finish** is cleared on every partition it was
reading. **Commit** is kept.

## Family

`TPartitionFamily` is a set of partitions that is always read together in
one session. That preserves message order for a single group (SourceId):

- split `0 → 1, 2` after Finish(0) with ScaleAware: the session that
  finished 0 locks `{0, 1, 2}`;
- merge `0 + 1 → 2` after Finish of both parents with ScaleAware:
  `{0, 1, 2}` in one session. With the old SDK, child 2 is a separate
  family.

After Commit the family can be split: 1 and 2 may move to other sessions
independently of 0.

A family is the unit of load-balancing. A third common session does not
hand 0 and 2 to different pipes while the parents are not committed.

## Split

1. While 0 is active, 1 and 2 are not locked.
2. Finish(0) without Commit on ScaleAware: 1 and 2 are locked **on the same
   session** as 0. With the old SDK the children are separate families; they
   may be kept off the same pipe (`BalanceToOtherPipe`).
3. Commit(0): 1 and 2 may be given to other sessions.
4. If 0 is read again (reread), 1 and 2 are taken away until 0 Finish or
   Commit again.

## Merge

2 can be read when both parents are inactive. 0 and 1 must share one
session only while children cannot be handed out separately
(`NeedReleaseChildren`: ScaleAware without Commit). Then the **parent**
families of 0 and 1 are merged: if they sit on different sessions, one is
released and they are glued together. This is not a merge of 0 and 2.

After Commit of both parents, 2 is a separate family; 0 and 1 may stay on
different sessions.

A parent family assigned to an explicit-partition session is not grown. It
can still be absorbed into a common parent family so the child can be read;
the special family moves to the common session (see
[Explicit partitions](#explicit-partitions)).

## Session change

While the consumer is alive (other sessions remain), an uncommitted family
moves as a whole: the new session gets the parent, and children are added
once the parent partitions have been finished.

If the **last** session closes, `TConsumer` is destroyed. Finish and Commit
are lost. The next session starts from the root: only 0 is given out until
it Finish or Commit of the last message again. After Finish(0) without
Commit, children go only together with 0; after Commit(0) children may move
to other sessions.

Finish, Started, and Commit events from an already dead pipe are dropped.

## Read session

"Auto-partitioning support" for a session in the code is the `ScaleAwareSDK`
flag on the partition from the Finish event, not a field of `TSession`.

`ScaleAwareSDK` guarantees that messages are given out for reading in the
order they were received from the server (including across partitions).

If `ScaleAwareSDK` is set, reaching the end of a partition is determined by
**Finish** — children can be given out immediately.

If the flag is absent (old SDK), **Finish** alone is not enough: it also
needs `StartedReadingFromEndOffset` (or **Commit**). Otherwise the children
are not readable and the heuristic runs:

1. After **Finish** the partition is taken from the session after a short
   delay and given to another session.
2. If the other session starts reading from the end (the partition has no
   data), the partition is treated as fully read
   (`StartedReadingFromEndOffset`).
3. If reading did not start from the end, after the partition is finished
   again it is taken from the session after a delay. Repeat, doubling the
   delay each time.

## Common assignment

Free families go to the least loaded sessions. Session load
(`LowLoadSessionComparator`) is `ActiveFamilyCount` first, then the size of
preferred groups. Assignment and later rebalance are by family count per
session.

Active and inactive partition counters order **families**
(`TPartitionFamilyComparator`), not sessions.

## Explicit partitions

A read session may list the partitions it wants (preferred groups). Those
partitions are locked even if their parents are not processed yet: they have
not been read to the end, and the last offset has not been committed.

A family currently assigned to such a session always contains exactly one
partition. Child partitions are not attached to it.

That family (A) may be merged into another family (B) that is **not**
assigned to an explicit-partition session. Then A moves onto B's session,
never the other way around. This is needed when partitions A and B were
merged and their child can be read only after the parent families are
combined.
