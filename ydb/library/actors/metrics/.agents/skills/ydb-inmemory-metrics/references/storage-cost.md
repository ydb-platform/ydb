# Estimate line storage before optimizing

Paths are relative to the repository root. Read current implementations in
`ydb/library/actors/metrics/lines/` before using these formulas after a format
change. Estimates below describe payload bytes, not total process memory.

## Choose a representation

| Representation | Benefits | Costs and limits |
|---|---|---|
| Raw scalar, fixed storage | Exact supported scalar encoding; simple fixed records; every observation retained | Timestamp and 64-bit value per sample, even for bool; repeats consume space |
| On-change scalar, fixed storage | Repeats need no record; exact supported values; step semantics | Same record cost per change as raw; no periodic heartbeat; gaps mean held value, not missing sampling |
| Static group, fixed storage | One timestamp for all fields; compile-time typed schema and reader; atomic sample | All fields written every time; 64-bit slot per field; schema fixed; unrelated frequencies waste space |
| Raw scalar, compressed storage | Small timestamp/value deltas; compile-time codec choices; periodic observations retained | Header/cache and full first value in every chunk; decoding cost; jumps may cost more than raw; decimal/time precision changes |
| On-change scalar, compressed storage | Avoid repeats and compress transitions | Same compression costs; equality uses rounded encoded value; subprecision changes disappear |
| Static group, compressed storage | Shared timestamp; per-field compile-time codecs; atomic full sample | Every field still encoded, even unchanged; larger header/cache; fixed schema; rollover writes all absolute values |
| Dynamic group, All | Runtime pool count; shared timestamp/chunks; mixed kinds and field labels | Immutable schema after creation; 1..128 fields; full vector every sample; mode byte; fixed 100 ms/decimal(2); no per-field codec selection |
| Dynamic group, OnChangeAll | No record when the whole vector is unchanged; full atomic changed sample | One changing field causes all fields to be encoded; caller still supplies and compares every field |
| Dynamic group, OnChangePartial | Only changed field IDs/values after the first sample; fewer physical lines/reserves | Mode/count/ID overhead; can exceed full mode when most fields change; caller comparison remains O(N), decoding reconstructs N values; first sample is always full |

Grouping shares storage, not logical identities. Keep each field's name and
labels. Group only fields with one writer and compatible collection/lifetime.
It reduces per-line metadata and spare chunks but couples retention/eviction;
a busy field can consume the group's history budget for quiet fields. Separate
lines allow independent writers, cadence and retention. A changed participant
set needs a new group generation; account for the full initial sample again.

## Codec tradeoffs

| Choice | Benefit | Cost or failure mode |
|---|---|---|
| Fixed storage | Preserves supported values without decimal quantization; predictable sizes | No delta savings, repeated values still use slots outside on-change |
| Compressed Absolute | Each value independent of the previous value | Always 9 value bytes, so larger than a fixed 8-byte value slot; timestamp compression may still help |
| UnsignedDelta | Full one-byte positive delta range 0..63 for counters | Decreases/resets escape to absolute; unsuitable for oscillating gauges |
| SignedDelta | Small increases and decreases, including counters with resets | ZigZag halves the positive compact range; large changes escape |
| Decimal(p) | Makes noisy floats compressible at explicit precision | Lossy; small rounded changes vanish in on-change; scaled signed range and finite-input checks can reject values |
| Nonzero timestamp step | Smaller time deltas, deterministic bounded time error | Loses substep time and can merge observation timestamps; does not reduce calls |
| Zero timestamp step | Exact process-cycle timestamps for typed compression | Typical deltas may use more bytes; dynamic groups do not expose this choice |

Raw/on-change typed frontends and static groups can choose a storage policy at
compile time. Dynamic groups fix their codec/time parameters and choose update
mode at registration. Avoid promising a configurable dynamic codec that the
current API does not provide.

## Payload cost model

Use actual `sizeof` for the target ABI; usual 64-bit sizes are shown below.
Let N be fields, K be records in a chunk, D be encoded timestamp bytes, and V_i
be encoded value bytes. Obtain usable payload P from chunk storage; do not
assume configured `ChunkSizeBytes` is all payload. Line/chunk descriptors,
reserved chunks, schema strings and snapshots are separate costs.

- Fixed scalar: header 8 bytes, record 16 bytes, including bool and float.
- Fixed static group: header 8 bytes, record `8 + 8*N` bytes.
- Compressed typed scalar/group: header `sizeof(THeader<N>)`, usually `16+8*N`;
  first record `8+9*N`; later record `D + sum(V_i)`.
- Dynamic group: header `sizeof(dynamic THeader)+8*N`, usually `16+8*N`;
  first record `1+8+9*N`; later full record `1+D+sum(V_i)`.
- Dynamic partial record after the first: `1+D+2+sum(1+V_i)` over changed fields.
  The 1 is mode, 2 is changed count, and each ID costs 1 byte. Unchanged
  on-change inputs normally publish no record; eviction can require unchanged
  state to be rematerialized. Do not count successful Append calls as records.

For compact nonnegative payload x, size is 1 byte for x<64, 2 for x<16384,
4 for x<2^30. Values outside the compact delta range use **9 bytes**, not 8:
zero tag plus a full 64-bit absolute value. Every first value is 9 bytes.
Absolute timestamps use 8 bytes (62-bit range); forward deltas use 1/2/4 bytes.
Backward time or a delta >=2^30 step units requires an absolute timestamp.

UnsignedDelta uses value-previous for nondecreasing counters; reset uses an
absolute value. SignedDelta ZigZag encodes d as `2*d` for d>=0 and `-2*d-1`
otherwise. The writer additionally requires magnitude <2^29; -2^29 escapes
even though its ZigZag payload fits 30 bits. Thus one-byte signed changes cover -32..31; two-byte changes cover
-8192..8191. Dynamic groups currently use SignedDelta even for unsigned fields;
do not estimate them using unsigned thresholds.

Decimal(p) rounds value*10^p to a signed integer before delta encoding.
For p=2 the rounding error is at most 0.005 in metric units (plus floating-point
representation error). Increasing p increases typical encoded delta sizes.
Nonfinite/out-of-range values are rejected. Choose the precision from the
consumer's needs, not from the smallest observed delta alone.

Timestamp step q quantizes absolute time down; error is less than q and does
not accumulate. At 1 Hz and q=100 ms, delta=10 costs 1 byte. With step zero,
deltas are exact process-clock cycles and may need more bytes. Quantization can
produce equal timestamps; preserve reader semantics and do not infer sampling
cadence from the storage step.

For constant later record cost R and first cost F, approximate chunk capacity:
`K = 1 + floor((P-H-F)/R)` if P>=H+F. Variable costs require summing actual
encoded records and restarting the model at each rollover. Average payload
cost is `(H+F+(K-1)*R)/K`; multiply by materialized records/second, not calls/second.
For on-change, measure change rate over representative intervals, including
bursts and resets. A quiet interval alone is not a retention estimate.

## Worked comparisons (steady state, before headers and rollover)

1. Five CPU fields sampled together each second, decimal(2), each signed scaled
   delta within -32..31: five fixed scalar lines cost 80 bytes/tick; fixed static
   group 48; compressed static group 6; dynamic All 7. If only one field changes,
   dynamic partial costs 6 versus full 7. If all five change, partial costs 14.
   A compressed raw scalar per pool costs 10 total but has five separate headers,
   first records and spare-chunk reserves. Verify CPU fluctuations meet the
   assumed delta bound; a 0.40 increase needs two value bytes at decimal(2).
2. A monotonic counter increasing by 100 each second with a 100 ms time step:
   fixed scalar 16 bytes/record; compressed UnsignedDelta 3 (time 1 + value 2).
   A reset or sufficiently large jump costs 10 (time 1 + absolute value 9).
3. Twenty flags with one toggle per second in the combined vector: separate
   fixed on-change lines write 16 bytes for that toggle; dynamic OnChangeAll
   writes 22; dynamic OnChangePartial writes 6. First group record is 189 bytes
   plus a 176-byte header; do not hide this cost for short-lived groups.

These are conditional examples, not measured producer results or guaranteed
compression ratios. A partial group beats full when
`2 + sum_changed(1+V_i) < sum_all(V_i)` for the same timestamp. For one-byte
values that is `2+2*m < N`. Compare physical allocation as well as record bytes.

## Optimization procedure and report

1. Inventory physical lines and logical fields, types, labels, writer ownership,
   cadence, group mode, codec and timestamp step. Distinguish periodic counters
   from gauges and on-change data. Establish required exactness and time error.
2. Capture representative history including load bursts, idle periods, resets,
   and participant changes. Measure materialized records/s, changed fields per
   record, timestamp deltas, scaled value deltas and absolute escapes. Record
   the interval and gaps; do not treat missing data as zero.
3. Calculate baseline and candidate payload bytes using the model above, including
   first records, headers, rollover, mode/count/IDs and escape frequency. Report
   assumptions and a worst-case scenario. Estimate metadata/reserves separately.
4. Measure actual committed payload and retained chunks in a controlled producer
   test or representative snapshot. Registry totals alone cannot isolate a
   producer. Compare equal intervals and semantic values; allocated capacity,
   committed bytes and whole-process memory are different quantities.
5. Estimate history as usable allocated payload divided by measured bytes/s,
   with rollover overhead. This is an upper estimate: the shared memory budget,
   spare/retiring/pinned chunks and global eviction can shorten it. Give a range,
   not a guaranteed per-line duration. Include burst/drop behavior.
6. Validate first sample, repeated values, chunk rollover, eviction-independent
   decoding, resets, time reversal, rounded changes and multiple participants
   using the relevant tests from the main skill. Check reader/UI semantics and
   exact integers. Do not add a broad build solely for an estimate.
7. Report baseline/candidate bytes per materialized record and bytes/s, physical
   lines/chunks, expected history range, precision/time error, CPU/read cost,
   and evidence. List rejected alternatives and their tradeoffs. Implement only
   candidates whose semantic and ownership constraints are satisfied.
