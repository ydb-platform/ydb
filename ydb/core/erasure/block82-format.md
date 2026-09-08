# Persisted block-8-2 codec

`Erasure8Plus2Block = 19` is append-only. Parts 1–8 contain data and parts
9–10 contain parity. The storage topology adds two handoff disks, which do
not participate in coding. This codec commit does not enable the storage
lifecycle or control plane by itself.

The persisted column size is 32 bytes, independent of the legacy `Prime=11`
arithmetic parameter. A block contains eight columns (256 bytes), and each
part's user region has `ceil(blobSize / 256) * 32` bytes. For `N=blobSize/32`,
the first `N%8` data parts contain `(N/8+1)*32` bytes; the others contain
`(N/8)*32`, with the final `blobSize%32` bytes appended to data part 8.
Data parts are consecutive ranges of the whole blob, zero-padded to equal
physical lengths.

ISA-L's systematic Vandermonde matrix uses GF(256), polynomial `0x11d`:

```
P0 = [1, 1, 1, 1, 1, 1, 1, 1]
P1 = [1, 2, 4, 8, 16, 32, 64, 128]
```

`CrcModeWholePart` appends the existing four-byte CRC32C to each user region
after encoding; CRC bytes never enter GF arithmetic. Empty blobs preserve
the existing block codec contract: zero-byte parts without CRC, four-byte
zero CRC parts with CRC. The VDisk envelope is outside this format.

Canonical and legacy APIs share one immutable ISA-L backend. It precomputes
45 source bases and 135 exact one/two-output plans and warms the runtime
dispatcher. Decode uses the first eight available parts, recovers only the
requested outputs, and never computes matrices or tables on the hot path.
The ISA-L 2.31 SVE pointer look-ahead is accommodated by an initialized ninth
source-pointer slot. Portable architectures use the ISA-L base aliases.

Canonical split retains data rope views and walks maximal common source
spans. Allocations for parity, padding and CRC use the caller's allocator.
An incremental quantum bounds bytes per part and need not be column-aligned.
Whole-only restore keeps temporary missing data out of unrequested output
slots. Fragments use one common part-relative interval excluding CRC bytes.
Diff/VPatch entrypoints explicitly reject this species until storage-level
Get+Put fallback is integrated.

`erasure_block82_ut.cpp` pins layout/CRC bytes against an independent oracle;
`erasure_isa_backend_ut.cpp` checks all loss/output plans, dispatched/base
equivalence, concurrent calls and a pointer-array guard page. Run:

```sh
./ya make --build relwithdebinfo -tA ydb/core/erasure/ut
./ya make --build relwithdebinfo -tA ydb/core/erasure/ut_perf
```

See `benchmark/README.md` for native paired characterization. The initial
implementation is validated locally on Linux x86_64. Native AArch64 tests
and performance runs require a runner and were explicitly deferred by the
requester; cross-compilation is not a substitute for runtime validation.
