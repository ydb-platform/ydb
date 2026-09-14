# Layout representation characterization

Build `./ya make --build relwithdebinfo ydb/core/blobstorage/groupinfo/benchmark`
and run measurements only after layout/ingress correctness tests pass. Building
may run alongside those tests. The `layout_bench` program calls exact production
layout methods; it contains no replacement layout algorithm.

The same harness can be copied into a detached worktree at the preceding
commit and built there. Run old/new Block42 and new Block82 with:

```sh
python3 ydb/core/blobstorage/groupinfo/benchmark/run.py \
  /path/to/baseline/layout_bench /path/to/current/layout_bench \
  /path/to/experiments/layout-128 --repeats 10 --iterations 1048576 --cpu 0
```

Thirty processes rotate their order. Each method has a fixed iteration count,
a deterministic 256-case corpus and warm-up. Results include raw wall/thread
CPU time, measured object size/alignment, median and MAD, plus paired Block42
cost ratios. Mutations include copying the value; CopyControl measures that
baseline. No numerical regression threshold or public ABI-size assertion is
introduced.

Add/Clear target part5 on handoff0; Get/Mask use a separate arbitrary-cell row5
corpus with bits3/4 set, exercising cells63/64 for stride12. This primitive
corpus is never passed to CountEffectiveReplicas. Matching cases use only valid
main/handoff placements and an independent augmenting-path oracle before and
after timing. The old executable rejects Block82 because it lacks capacity.

No-heap ownership is checked from production source: `TBitMap<128,ui64>` uses
`TFixedStorage::Data[2]`, and the matching temporary has sufficient inline
capacity for ten rows. The report does not claim that allocation counters were
instrumented. Size/alignment are measured, not asserted as persistent ABI.
