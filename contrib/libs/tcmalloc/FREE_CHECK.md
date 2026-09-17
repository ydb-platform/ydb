# Diagnostic free quarantine

This branch enables a bounded quarantine in `FreeSmall` for all tcmalloc
variants built from `common.inc`. No runtime switch or application changes are
required. Rebuild the target application from this branch.

The quarantine holds at most 4096 small allocations with size-class capacity
at most 32 KiB (up to 128 MiB retained in the table, plus metadata and transient
evictions). Each pointer hashes to one slot protected by a kernel-only spinlock.
A hash collision verifies and evicts the previous object. Actual deallocation
happens after unlocking. The allocator's existing fork support also locks this
table when fork support is enabled.

On free, the first and last 64 bytes are filled with an address/offset-dependent
pattern. Eviction checks this pattern. A mismatch aborts with
`Free check: write after free`, pointer, offset and capacity. A repeated free of
an object still in its slot aborts with `Free check: double free` and its pointer.
The crash stack is the detecting free, not necessarily the corrupting access or
original free. No allocation/free stack history is collected.

## Coverage and limits

- Covers ordinary small `free`, sized/aligned delete and realloc's old-object
  release when they reach `FreeSmall`.
- Large, sampled and SelSan-tagged allocations keep their existing behavior.
- Only writes to the checked bytes are detected; reads, writes to the middle of
  larger objects, and writes that restore the same pattern are invisible.
- Detection ends on eviction. There is no minimum retention time, no periodic
  scan, and no exit-time flush. An object left in the table may never be checked.
- Repeated free after eviction or after address reuse is not reliably detected.
  This is a diagnostic aid, not a complete memory-safety checker.
- Extra work is one hashed slot lock and bounded poison/verify accesses per
  eligible free. Allocation has no new checks. CPU overhead has not been measured;
  the retained-memory bound does not imply negligible RSS or latency impact.
- Poison checking changes timing and reuse, so it can change bug reproducibility.

## Validation

Run on Linux through the project's remote build tooling:

```
ssh_ya_pool ya make --build relwithdebinfo -tA library/cpp/malloc/tcmalloc/free_check_ut
```

The tests exercise double free (including sized free), head/tail corruption,
concurrent legitimate churn, and fork with allocator fork support enabled.
