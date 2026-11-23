# In-memory metric concurrency models

The models cover retirement ordering, snapshot publication, and pinned chunk
reuse. Each has explicit finite bounds and uses full-state verification.

## Chunk retirement

`chunk_retirement.pml` models `RetireChunk`, `ReleasePinnedChunk`,
`ReturnChunkToFree`, snapshot pinning, and chunk reuse in PR #36913 at
`8fb1ff26d3933747ac8530841dc82f80f333d9f8`.

The C++ implementation uses the State-first retirement ordering checked by this
model. The snapshot-publication fix is separate.

### Compared orders

- `STATE_FIRST=0`: original order, CAS `Readers` to the retiring range, then store
  `State = Retiring`.
- `STATE_FIRST=1` (default): store `State = Retiring` before loading and CASing
  `Readers`. The successful CAS can transfer reclamation to the last reader;
  the retiring thread must not subsequently write the chunk's state.

The C++ fix moves this statement to the start of `RetireChunk`:

```cpp
chunk->State.store(EChunkState::Retiring, std::memory_order_release);
```

The remaining load/CAS loop and the `readers == 0` branch stay unchanged.
`TryStealOldestChunk` has already unlinked the chunk (under the original line lock), so no
new snapshot can pin it. Readers that finish before the CAS decrement a
nonnegative count; if they all finish, the retiring thread performs reclamation.
Readers that finish after the CAS use the existing last-reader reclamation path.
The release CAS and acquire RMW also order the preceding State store before
last-reader reclamation in C++; Spin itself checks only SC interleavings.

### Scope and properties

One physical chunk is retired twice, with one reuse between retirements. Two
independent evictors compete for the heap entry. There are 0 to 3 existing pins
and an optional concurrent snapshot which can pin either incarnation.

Atomic blocks represent individual RMW operations or critical sections under
the original line/victim locks, with ghost accounting updated alongside them.
Those locks describe the historical implementation modeled here, not the
current actor-owned backend.
The load, CAS and State store in retirement remain separate scheduling steps.
CAS retries model changed expected values; spurious failures are stuttering.

Assertions check:

- old retirement cannot overwrite a returned or reused chunk;
- reclamation and reuse happen only after all pins have been released;
- a pin cannot outlive its incarnation;
- free-list/heap membership and return accounting remain consistent;
- each incarnation is returned exactly once.

Safety runs also check invalid end states. `live_reclaimed` checks that both
retirements eventually return the chunk. The finite model has no perpetual
producer or polling loop, so no fairness flag is needed for this property.

Payload publication, arbitrary repeated reuse, line deletion, full victim-heap
ordering, and the stealing thread's post-retirement free-list fast path are
outside the model. Passing does not prove the entire backend or weak-memory
behavior correct.

### Reproduce

Use an empty temporary directory so generated verifier files stay outside the
source tree. Set `model` to the absolute path of `chunk_retirement.pml`:

```bash
run_dir=$(mktemp -d)
cp "$model" "$run_dir/chunk_retirement.pml"
cd "$run_dir"
spin -DSTATE_FIRST=1 -DINITIAL_READERS=2 -a chunk_retirement.pml
cc -O2 -DMEMLIM=1024 -DNOREDUCE -DSAFETY -DNOCLAIM -o pan pan.c
./pan -m200000 -w20 > safety.log 2>&1
cc -O2 -DMEMLIM=1024 -DNOREDUCE -o pan pan.c
./pan -a -m200000 -w20 > liveness.log 2>&1
```

Repeat with `INITIAL_READERS=0,1,3`. For the original order, generate with
`STATE_FIRST=0` and run safety; replay the counterexample with the same defines:

```bash
spin -DSTATE_FIRST=0 -DINITIAL_READERS=2 -t -p -g -l chunk_retirement.pml
```

Record the model SHA-256, Spin/compiler versions, host, flags, and full output
for each run. A verifier exit code alone is not proof of success: check
`errors: 0` and that the exhaustive search finished without a resource limit.

### Verified results

Spin 6.5.2; full-state search with `-DNOREDUCE`, no bitstate hashing.
Both safety and liveness completed for every fixed-order configuration below.

| Order | Existing readers | Safety stored states | Liveness stored states | Result |
|---|---:|---:|---:|---|
| CAS first | 2 | 204 before counterexample | not run | violated |
| State first | 0 | 713 | 692 | holds |
| State first | 1 | 6,427 | 6,367 | holds |
| State first | 2 | 74,625 | 74,526 | holds |
| State first | 3 | 1,129,214 | 1,129,076 | holds |

The original-order trail transfers reclamation to the last reader, returns the
chunk, reuses and seals it in the writer, then resumes the old State store.
The epoch assertion fails and the new heap entry now refers to a Retiring chunk.

## Snapshot publication

`snapshot_publication.pml` separates payload writes, mutable header updates,
commit publication and reads. One writer appends up to `RECORDS` records while
one reader traverses a snapshot twice. `SNAPSHOT_PREFIX=1` captures the committed
length once; `SNAPSHOT_PREFIX=0` reads the mutable header like the original code.

Assertions require every visible record to be initialized and the snapshot's
record count to stay unchanged. This abstracts acquire/release as an SC
publication boundary; the model does not detect C++ data races or simulate weak
memory. Pinning is deliberately handled by the next model.

With `RECORDS=5`, the fixed version completes safety with 1,952 stored states and
0 errors. The original version (`RECORDS=3`, `SNAPSHOT_PREFIX=0`) yields a trail
where the second traversal sees appended records in the same snapshot.

## Pinned chunk reuse

`pinned_chunk_reuse.pml` covers two physical chunks, three publications, two
concurrent snapshots and an evictor. Each snapshot may pin both chunks and hold
them while the producer and evictor run. Incarnation markers make overwrites of
borrowed data observable. Selection abstracts the oldest-victim policy by
allowing either sealed chunk. Completed payload publication is collapsed here
and checked separately in the publication model.

Assertions check that pinned chunks are never returned or overwritten, reader
counters do not underflow, snapshot data keeps its incarnation, and each
incarnation is returned exactly once. `live_all_returned` checks reclamation of
all three publications when the finite readers and writer finish.

`PIN_READERS=0` omits the reader counter as a negative control, and produces a
counterexample to safe reclamation. It is not an additional bug claim about the
C++ implementation, which already maintains this counter.

Default bounds complete safety with 2,911,591 stored states and 0 errors, and
liveness with 2,873,411 stored states and 0 errors. No fairness is needed: the
model has finite producers and readers and no polling self-loops.

### Run the additional models

Use a fresh temporary directory and copy the selected model into it. Generate
and compile using the same flags as above, replacing the generation command:

```bash
spin -DSNAPSHOT_PREFIX=1 -DRECORDS=5 -a snapshot_publication.pml
# Or, in a different temporary directory:
spin -DPIN_READERS=1 -DREADERS=2 -DWRITES=3 -a pinned_chunk_reuse.pml
```

For safety use `-DSAFETY -DNOCLAIM`; for the reuse liveness claim omit those flags
and run `./pan -a -m200000 -w22`. Use `-DNOREDUCE` and `-DMEMLIM=1024` in both
builds. The publication model has no LTL claim. To reproduce negative controls,
set `SNAPSHOT_PREFIX=0` or `PIN_READERS=0` respectively; replay with `spin -t -p
-g -l` and the same defines and model filename. All reported runs use Spin 6.5.2
and complete full-state search unless explicitly described as a counterexample.

## Historical free chunk bitmap

`free_chunk_bitmap.pml` models two workers sharing a two-bit free-chunk word,
with two allocation attempts per worker. A successful CAS clears one bit and
acquires exclusive ownership. Cleanup completes before a release RMW sets the
bit again. This historical design replaced the free-list mutex; current allocation uses an owner-only free deque and funnel return queue.
Pinning and retirement are covered separately by the existing models. Their
atomic free-list operations now abstract bitmap ownership acquisition/publication.

Spin 6.5.2 full-state runs (`-DNOREDUCE`, `-DMEMLIM=1024`, `-m200000 -w22`):
safety completes with 3,086 stored states, 2,847 matched, 5,933 transitions,
depth 80 and zero errors; `live_returned` completes with 3,084 stored states,
6,168 visited, 8,749 matched, 14,917 transitions, depth 153 and zero errors.
Generate using `spin -DUSE_CAS=1 -a free_chunk_bitmap.pml`; compile safety with
`-DSAFETY -DNOCLAIM`, or omit those flags and run with `-a` for liveness.
`USE_CAS=0` is a negative control: an unchecked bitmap store loses a concurrent
return, violating final free-chunk accounting. These are finite SC checks, not
proofs of C++ weak-memory behavior, wait-freedom or the complete backend.

The backend now has one actor owner; the synchronous writer path and all
service mutexes have been removed. See [ownership and API](../README.md).
The retirement model below retains its historical competing-evictor abstraction;
the current single owner serializes selection and snapshot construction. Its
last-reader handoff and the bitmap publication obligations still apply.

## Per-line chunk reserve and coalesced refill

`chunk_reserve.pml` models two SPSC rings of capacity two, two consumers (three
pops each), and one manager. Slot access, index publication, flag exchange and
message send are separate transitions. It checks FIFO, capacity, safe slot
reuse, and eventual refill with reliable delivery and weak process fairness.
The C++ implementation uses release/acquire index handoffs and an acq_rel
exchange on every notification attempt; the manager exchanges the flag to false
before scanning. This is a finite SC abstraction, not a C++ weak-memory proof.
Allocation failure, eviction and shutdown are outside this model.

Spin 6.5.2, `-DNOREDUCE -DMEMLIM=1024 -DNFAIR=3`, `-m200000 -w22`:

- Safety (`-DSAFETY -DNOCLAIM`): 317,976 stored states, 609,041 matched,
  927,017 transitions, depth 233, errors 0; exhaustive search completed.
- Liveness (`-a -f`): 317,969 stored / 3,076,003 visited, 8,105,233 matched,
  11,181,236 transitions, depth 527, errors 0; exhaustive search completed.
- Negative control (`spin -DRESET_AFTER=1 -a`): a writer consumes after its
  queue was scanned, observes the old flag, and sends nothing; the manager then
  clears the flag. The reserve remains short with no message pending. The final
  refill assertion fails at depth 190.

Generate and compile `pan.c` in a temporary directory, as for the other models.

## Pending registration versus Close

`registration_close.pml` models a pending writer and manager with a bounded two
writes. Reader initialization precedes the Pending-to-Ready CAS. Close is in the
writer process, so it cannot overlap that writer's Append. The model checks
initialized ownership on access, no resurrection after Close, and eventual
reclamation after both processes finish. Delivery is reliable; shutdown,
admission bounds, payload and C++ weak-memory behavior are outside this model.

Spin 6.5.2, `-DNOREDUCE -DMEMLIM=1024 -DNFAIR=3`, `-m200000 -w22`:

- Safety (`-DSAFETY -DNOCLAIM`): 93 stored, 43 matched, 136 transitions,
  depth 22, errors 0; exhaustive search completed.
- `closed_reclaimed` (`-a -f`): 85 stored / 457 visited, 450 matched,
  907 transitions, depth 39, errors 0; exhaustive search completed.
- Negative control (`spin -DUSE_READY_CAS=0 -a`): unconditional publication
  revives a handle closed between initialization and publication; assertion
  violation at depth 8.

## Sealed-to-released queue handoff

`chunk_handoff.pml` models one chunk, a writer publishing Close/seal, a snapshot
releaser and the metadata owner. Queue publication/detachment are abstracted;
internal funnel links, weak memory and the notification shutdown gate are excluded.
A pin owned by the seal queue prevents admission from recycling a queued node.
The manager can retire before or after detaching that node. Exactly one final
return is asserted, and `live_reclaimed` requires eventual reclamation.

Verified with Spin 6.5.2, `cc -O2 -DNOREDUCE -DMEMLIM=1024`, `pan -m200000 -w20`:
safety (`-DSAFETY`) 27 stored states, 40 transitions, depth 29, errors 0;
fair liveness (`-a -f`) 27 stored / 125 visited, depth 29, errors 0.
`spin -DQUEUE_PIN=0 -a model.pml` is the negative control: reuse while still in
Sealed fails an assertion at depth 16. The queue pin is required even when the
writer has already closed the line. Regenerate pan in a separate build directory.

## Per-line maintenance requests

`line_refill.pml` models two requests from one writer and one manager. It verifies
that a producer never overwrites the queue-owned lifetime slot, and that the last
request is eventually observed. Queue publication/detachment are abstract; queue
internals, registry membership and C++ weak memory are excluded.

Spin 6.5.2, `cc -O2 -DNOREDUCE -DMEMLIM=1024`, `pan -m200000 -w20`:
safety (`-DSAFETY`) 125 stored states, 196 transitions, depth 65, errors 0;
fair liveness (`-a -f`) 125 stored / 843 visited, depth 65, errors 0.
Negative control `spin -DRESET_BEFORE=0 -a model.pml` resets the coalescing flag
after processing and loses the final request (assertion failure at depth 46).
Generate and compile pan in a separate build directory.
