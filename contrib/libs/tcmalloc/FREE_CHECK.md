# Offset-generation diagnostic allocator

This branch replaces user allocations with persistent aligned slots. It is an
experimental diagnostic allocator, not a production performance replacement.
Rebuild the application from this branch; no application API changes are needed.

## Layout and lifetime

A pool has a fixed power-of-two slot size S and alignment A. Each slot starts
with a 48-byte header on the supported 64-bit targets. Its user capacity is S/2.
The first pointer is `base + align_up(sizeof(Header), A)`. Successive allocations
advance the offset by A, independently of changes in requested size. The last
position is at offset S/2. The minimum slot size is 256 bytes, and pool geometry
always permits at least two positions.

On free, the allocator finds an immutable registered range before touching the
header, masks the address to obtain the slot base, validates the header checksum,
state and current offset, and changes state under the owning pool's spinlock.
A stale pointer to a currently allocated slot fails with STALE_GENERATION_FREE.
Repeated free of a free slot fails with DOUBLE_FREE. Any free of a quarantined
slot fails with FREE_DURING_QUARANTINE. Interior pointers fail offset validation.
The header checksum detects accidental corruption; it is not a security boundary.

After the last position is freed, the whole slot enters a FIFO quarantine with a
monotonic deadline. A subsequent allocation from that pool drains at most eight
expired entries. Expiration resets the offset. No slot returns early to satisfy
memory pressure. The allocator obtains new backing within its budget or applies
the existing malloc/new OOM policy. Zero quarantine time is permitted explicitly.

Pools are sharded eight ways by allocating thread ID. Cross-thread free uses the
slot's original pool. Backing comes from unsampled tcmalloc page allocations;
registry metadata uses mmap. Slots larger than 2 MiB over-reserve backing
to align the slot within it; the padding is also charged to the budget. Backing ranges, their class and registry entries
remain alive for the process lifetime. They are never handed to ordinary object
free lists or reassigned to another class. This avoids erasing generation history
and allows lock-free lookup of immutable registered ranges. Fork support includes
population and pool locks when the existing allocator fork support is enabled.

## Startup settings

Read once before the first allocation; changing the environment later has no effect.
All values are unsigned decimal integers. Invalid values abort startup.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TCMALLOC_GENERATION_BUDGET_BYTES` | 17179869184 (16 GiB) | Bound on reserved slot backing plus mapped range-registry storage |
| `TCMALLOC_GENERATION_QUARANTINE_MS` | 1000 | Minimum time before reuse after a complete offset cycle |
| `TCMALLOC_GENERATION_POISON` | 0 | Nonzero enables first/last 64-byte poison checks for the last request when entering/leaving quarantine |

The budget is not an RSS cap: static tables, ordinary tcmalloc metadata and page
allocator overhead are additional. Empty pool backing is retained, so class and
alignment diversity increases the high-water mark. A 16-byte request occupies a
256-byte slot; memory overhead is substantial even with poison disabled.

Read-only numeric properties are `tcmalloc.generation.reserved_bytes`,
`tcmalloc.generation.live_requested_bytes`, and
`tcmalloc.generation.quarantined_bytes`. Their concurrent snapshot is approximate.
Existing generic tcmalloc statistics describe backing allocations, not live slot
requests. Querying live bytes locks pools in turn; do not poll it on a hot path.

## Diagnostics and coverage

Failure writes a fixed-size, allocation-free message to stderr, then aborts.
The record includes reason, pointer, slot base, expected pointer, slot size,
current offset, requested size and numeric state (free=0, allocated=1,
quarantined=2). A corrupt header is not trusted for the remaining fields.
The detection stack is available in a core dump when enabled by the environment.
Allocation and original-free stacks are not recorded.

The integration covers malloc/new, calloc, aligned allocation, sized/aligned
free/delete, realloc, usable-size/size-returning APIs, nallocx and heap ownership.
Realloc always allocate-copies-frees; failure preserves the old allocation.

Known limits:

- The original pointer is numerically valid again after a full cycle and its
  quarantine. An older dangling pointer cannot then be distinguished.
- Reads and writes through stale pointers are not instrumented. Offset ranges
  overlap, so a stale write can corrupt the current allocation.
- Optional poisoning detects only changes to the checked parts of the final
  request while in quarantine, at drain time. No background/exit scan exists.
- Heap/allocation/lifetime profiles return unavailable in this mode; object-range
  tracing returns Unimplemented. GWP-ASan sampling is bypassed. SelSan builds are
  rejected unless generation mode is disabled.
- Hot/cold hints are not honored, and slots do not preserve per-request NUMA
  placement. This diagnostic backend uses the backing allocation's placement.
- Normal free checks do not additionally validate a caller-provided sized-delete
  size or alignment. Actual geometry comes from the registry.
- Extended stack-history mode and releasing empty backing ranges are not implemented.

## Build, tests and comparison

Remote validation:

```
ssh_ya_pool ya make --build relwithdebinfo -tA library/cpp/malloc/tcmalloc/free_check_ut library/cpp/malloc/tcmalloc/free_check_ut_percpu
ssh_ya_pool ya make --build relwithdebinfo library/cpp/malloc/tcmalloc/generation_bench
```

The test allocator has an injected clock and independent pools; tests do not
sleep or rely on chance address reuse. Death tests require SIGABRT and the exact
reason rather than accepting any crash. Existing YDB actor utility tests provide an
additional compatibility check, not a production workload benchmark.

Build the benchmark again with `-DTCMALLOC_GENERATION_DISABLED=yes` for the ordinary
tcmalloc baseline. This is a build-time choice for the whole linked allocator;
there is no runtime format switch. The benchmark reports elapsed time per
malloc/free pair, sampled p99 latency, maximum RSS and diagnostic reserved bytes.

The CPU-capable benchmark variant is selected with `-DGENERATION_BENCH_PERCPU=yes`.
Measured results and validation scope are recorded in
`library/cpp/malloc/tcmalloc/generation_bench/VALIDATION.md`.
