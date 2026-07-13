# Design: functional equivalence tests (`ydb/tests/hash_test --functional`)

Implementation spec for the functional mode described in `README.md` ("Equivalency of
different hash / encryption implementations"). It complements `ai.md` (the `--perf`
design) and assumes the perf harness as currently implemented: `kernels.h` registry,
`bench.cpp` with `RunCipherSelfCheck`/`RunHashSelfCheck`, pinned XXH3 TUs
(`kernel_xxh_sse2.cpp`, `kernel_xxh_avx2.cpp`), `main.cpp` CLI on `NLastGetopt`.

## 1. Goal

Two capabilities, both under `--functional`, both multi-threaded via `--threads`:

1. **In-memory equivalence sweep** (default when neither `--output` nor `--input` is
   given): verify that all implementations within a family produce identical results —
   `ChaCha` == `ChaChaVec` == `ChaCha512`, and `xxh3-scalar` == `xxh3-sse2` ==
   `xxh3-avx2` == `XXH3_64bits_dispatch` — over randomized lengths, alignments, and
   call-splitting patterns. This is `RunCipherSelfCheck`/`RunHashSelfCheck` "on
   steroids": those check one 64 KiB buffer once; this sweeps the interesting
   boundaries and keeps fuzzing for a configurable duration.
2. **Cross-machine comparison via a file**:
   * `--output FILE --output-size G`: generate `G` GiB of deterministic data, hash
     every 4 KiB block with all variants (cross-checking them — goal 1 rides along for
     free), and persist "hash page -> data blocks" groups into a preallocated,
     memory-mapped file, work spread across threads.
   * `--input FILE`: memory-map an existing file (possibly written on a different
     machine), recompute every hash with **every implementation variant supported on
     this machine**, and compare against the persisted records.

Exit code 0 = everything matched; 1 = any mismatch or I/O/validation error. Mismatches
must be reported with enough detail to reproduce (seed, block index, variant, offset).

## 2. Non-goals

* No performance measurement here; no pinning, no affinity, no bandwidth math.
  Threads are for wall-clock speed of the check only.
* File mode needs POSIX `mmap`; it works on Linux and macOS (x86-64 and ARM64), just
  not Windows (`#if !defined(_win_)`, see §2a). The in-memory sweep works everywhere
  the ciphers compile, which today is also everywhere except Windows.
* No streaming/incremental XXH3 state verification (`XXH3_..._update`); only one-shot
  hashing, matching what the perf kernels use.

## 2a. ARM64 (Apple Silicon) support

Both modes of `--functional` work on ARM64, not just x86-64. This is what actually
makes the ai_func.md §1 goal-2 cross-machine workflow interesting: write the hash file
on an x86 box, `scp` it to an Apple Silicon Mac, and verify there (or vice versa) --
any SSE2/AVX2/NEON/scalar divergence in XXH3, or any way ChaCha's integer path could
disagree with itself across compilers/architectures, shows up as a concrete mismatch.

* **Variant tables shrink, they don't disappear.** On ARM64, `GetXxh3Variants()`
  returns `{xxh3-scalar, xxh3-neon}` (see ai.md §6.4 for the NEON kernel) and
  `GetChaChaVariants()` returns `{chacha-int}` only -- `chacha-sse`/`chacha-avx512` and
  `xxh3-sse2`/`xxh3-avx2`/`xxh3-dispatch` stay x86-only (`ChaChaVec`'s vectorized
  implementation is excluded from ARM64 builds at the `ydb/core/blobstorage/crypto`
  level; not something this test controls). The equivalence sweep and file mode both
  read these tables generically, so no driver code needed to change -- fewer variants
  just means fewer things to cross-check per iteration/block.
* **File mode's `_arm64_` exclusion was accidental**, not a real platform gap: mode 2
  only needs POSIX `mmap`/`open`/`fstat`, all available on macOS. The one genuine
  difference is Linux's `fallocate()`, which doesn't exist on macOS at all (not just
  "unsupported", not declared) -- `TMappedFile::OpenForWrite` skips straight to
  `ftruncate` under `_darwin_` instead of trying `fallocate` first. Practical
  consequence: on macOS, `ENOSPC` during a big `--output` surfaces as SIGBUS mid-write
  rather than failing eagerly at open time, same as the existing "fallocate failed,
  falling back to ftruncate" path already accepts on Linux filesystems that don't
  support it.
* **Prerequisite fix**: the vendored `ydb/tests/hash_test/xxHash` library used to
  unconditionally compile `xxh_x86dispatch.c`, which `#error`s on any non-x86 target --
  this alone made the whole binary fail to build on ARM64. Its `ya.make` now excludes
  that file under `IF (NOT ARCH_ARM64)`.
* **`kernel_xxh_scalar.cpp` was wrongly grouped** with the x86-only kernels in the old
  `ya.make` (`IF (NOT OS_WINDOWS AND NOT ARCH_ARM64)`) even though it's pure scalar C.
  That meant `Xxh3Variants[]` (§4.3) was an empty array on ARM64 and the equivalence
  sweep silently checked zero xxh3 variants there. Fixed: `xxh3-scalar` now builds
  everywhere except Windows.

## 3. CLI

```
./hash_test --functional
    [--threads N]          # default: number of online CPUs (NSystemInfo::CachedNumberOfCpus())
    [--seed S]             # ui64, default 1; all randomness derives from it (reproducibility)
    [--duration-s D]       # in-memory sweep length, seconds, default 10
    [--output FILE]        # write mode
    [--output-size G]      # total file size target, GiB, double (e.g. 0.5), required with --output
    [--input FILE]         # verify mode
    [--verify-data]        # with --input: also re-generate PRNG data and compare raw blocks (needs same seed semantics, see §6)
```

Validation in `main.cpp`:

* `--perf` and `--functional` are mutually exclusive.
* `--output` and `--input` are mutually exclusive; `--output` requires `--output-size > 0`.
* `--threads >= 1`.
* Reuse the existing startup self-checks before any functional work: call
  `RunStartupSelfChecks()` (feature printout) — but **not** the abort-on-mismatch
  cipher/hash self-checks; the functional sweep subsumes them and reports better.

## 4. Code layout and per-variant entry points

New files:

```
func.h        # public API for main.cpp: RunFunctionalSweep / RunFileWrite / RunFileVerify
func.cpp      # sweep driver, file format, writer/verifier, thread pool
variants.h    # THashVariant / TCipherVariant tables (see below)
variants.cpp  # table definitions + the always-available entries (city, dispatch, scalar chacha)
kernel_xxh_scalar.cpp   # NEW pinned scalar XXH3 (see 4.2)
```

### 4.1 Why new entry points instead of reusing `TKernelDesc`

The perf kernels are the wrong shape for functional testing:

* The chacha kernels encrypt **in-place** and **continue the keystream** across calls
  (hidden mutable state in ctx) — useless for "same input -> same output" comparison
  and for block-addressable file records.
* `TKernelFn` forces `wsSize % blockSize == 0` and returns an XOR-folded `ui64`; the
  sweep needs odd lengths (0, 1, 65, 4095, ...) and raw outputs.

The pinned XXH3 code, however, lives as `static` functions inside the kernel TUs
(`XXH_INLINE_ALL`), so the functional code cannot call it without new exported
wrappers. Add to each pinned TU a one-shot export (2 lines each):

```cpp
// at the bottom of kernel_xxh_sse2.cpp
ui64 XxhSse2HashOneShot(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}
```

Same in `kernel_xxh_avx2.cpp` (`XxhAvx2HashOneShot`) and the new
`kernel_xxh_scalar.cpp` (`XxhScalarHashOneShot`). Declare them in `variants.h`.
**Keep the AVX2 TU minimal** — the wrapper is trivially fine, but do not put table or
driver code into a `-mavx2` TU (same rule as ai.md §6.4: nothing in that TU may run
before the runtime gate).

### 4.2 New kernel TU: `kernel_xxh_scalar.cpp` (small, justified extension)

A pinned scalar XXH3 (`#define XXH_VECTOR 0`, `XXH_INLINE_ALL`) gives the family a true
non-SIMD reference implementation — without it, "sse2 == avx2" could both be wrong the
same way only in theory, but the scalar reference costs ~20 lines and closes the
question. Mirror `kernel_xxh_sse2.cpp` exactly (op struct + `RunLoop` + `TKernelDesc
KernelXxhScalar{"xxh3-scalar", nullptr, ...}` + `XxhScalarHashOneShot`). Register it in
`kernels.h` and `AllKernels` in `bench.cpp` — the perf table gets a scalar row for
free, which the README's integer-vs-vector comparison actually benefits from.

### 4.3 Variant tables (`variants.h` / `variants.cpp`)

```cpp
struct THashVariant {
    const char* Name;                        // "xxh3-scalar", "xxh3-sse2", "xxh3-avx2", "xxh3-dispatch", "xxh3-neon"
    bool (*IsSupported)();                   // nullptr => always
    ui64 (*Hash)(const void* data, size_t len);
};
// Table order = reference first (scalar). city64 is NOT here: it has a single
// implementation, nothing to cross-check; it participates only in file records (§6).
// x86-64: {scalar, sse2, avx2, dispatch}. ARM64 (§2a): {scalar, neon}.
const THashVariant* GetXxh3Variants(size_t* count);

struct TCipherVariant {
    const char* Name;                        // "chacha-int", "chacha-sse", "chacha-avx512"
    bool (*IsSupported)();
    // Fresh cipher per call: SetKey(key,32); SetIV(iv, (const ui8*)&counter64);
    // then Encipher(src, dst, len). Deterministic, position-independent,
    // safe to call concurrently from many threads (no shared state).
    void (*Encipher)(const ui8 key[32], const ui8 iv[8], ui64 counter64,
                     const ui8* src, ui8* dst, size_t len);
};
const TCipherVariant* GetChaChaVariants(size_t* count);
```

Notes:

* `counter64` is the ChaCha 64-byte-block counter passed via the two-argument
  `SetIV(iv, blockIdx)` overload (the same seek mechanism `TStreamCypher` uses in
  `ydb/core/blobstorage/crypto/crypto.cpp`). All three classes support it.
* Constructing a cipher object per call is fine here — correctness mode, and key setup
  is trivial next to hashing 4 KiB.
* `ChaCha512` has **no `Decipher`** — the round-trip check (§5) must decrypt by calling
  `Encipher` again (stream cipher symmetry), which works for all three uniformly.
* Gates: `chacha-avx512` and `xxh3-avx2` use `NX86::CachedHaveAVX512F` /
  `CachedHaveAVX2`; check the gate before *constructing* `ChaCha512` (SetKey/SetIV
  touch `vec512` state — same pitfall as ai.md §6.3).
* `"xxh3-dispatch"` wraps `XXH3_64bits_dispatch` from the vendored library — it is what
  production code calls, so it belongs in the equivalence set.

## 5. Mode 1: in-memory equivalence sweep

### 5.1 Structure

`RunFunctionalSweep(threads, seed, durationS)`:

* Spawn `threads` workers (`std::thread`). Shared state: `std::atomic<bool> Failed`,
  `std::atomic<ui64> IterationsDone`, a mutex for error printing. Workers stop when
  `Failed` is set or the deadline passes.
* Worker `t` seeds a local SplitMix64 PRNG with `seed * 0x9E3779B97F4A7C15ULL + t`
  (distinct, deterministic streams; document that `--seed S --threads N` reproduces a
  failure only with the same N — print both in the failure message).
* **Phase A (deterministic, each worker does the full list once — it's cheap):** sweep
  the boundary lengths below at offsets {0, 1, 8, 63} within a 64-aligned buffer.
* **Phase B (randomized, until deadline):** each iteration picks a random length and a
  random offset in [0, 63], fills the buffer from the PRNG, runs all checks.

Boundary lengths (Phase A, and the distribution buckets for Phase B):

```
XXH3 code-path edges:   0, 1, 3, 4, 8, 9, 16, 17, 32, 64, 96, 128, 129, 160,
                        240, 241, 256, 512, 1024, 4096   (XXH3 switches impls at 16/128/240;
                        >240 enters the SIMD "hashLong" path — the one we actually pin)
ChaCha edges:           63, 64, 65, 127, 128, 191, 192, 193, 255, 256, 257, 320, 321
                        (CHACHA_BPI is 3 or 4 => vector loop strides 192/256 bytes,
                        plus 64-byte block tails)
Large (Phase B only):   random in [4 KiB, 8 MiB], log-uniform — exercises the
                        multi-iteration vector loops and page crossings
```

Phase B bucket mix: 40% XXH3/ChaCha edge list (jittered ±3 bytes, clamped to >= 0),
40% uniform [0, 4096], 20% large log-uniform.

### 5.2 Checks per iteration (given `data`, `len`, all on this thread's buffers)

1. **XXH3 family**: compute `Hash(data, len)` with every supported variant from
   `GetXxh3Variants`; all must equal the first (scalar reference).
2. **ChaCha out-of-place equivalence**: fixed key (the same 0x00..0x1f pattern already
   used in `bench.cpp`), fixed IV = zeros, `counter64` drawn from the PRNG (exercises
   nonzero stream positions, which the current self-check never does). Each supported
   variant enciphers `data -> outVariant`; all ciphertexts must be byte-identical
   (memcmp against the `chacha-int` reference).
3. **In-place**: one randomly chosen variant enciphers a copy of `data` with
   `src == dst`, result must equal the out-of-place ciphertext. (Perf mode relies on
   in-place; keep it continuously verified.)
4. **Round-trip**: encipher the ciphertext again with the same key/IV/counter (stream
   cipher symmetry) using a randomly chosen variant; must recover `data` exactly.
5. **Streaming-split equivalence** (important — this is the mode the perf kernels use
   and the current self-checks never cover): create one cipher per variant with
   `SetIV(iv)` (counter 0), then encipher `data` as a sequence of random-size segments
   (sizes drawn until `len` is consumed, including odd sizes so segments end mid
   64-byte block if the impl allows it*) on the *same* cipher instance. The
   concatenated output must equal the single-call `Encipher(data, out, len)` of the
   reference. (*If a mismatch shows the implementations only agree when every segment
   except the last is a multiple of 64 — the classic stream-cipher partial-block-state
   caveat — constrain the generated split sizes to multiples of 64 and **document the
   constraint in README**; the perf kernels only ever pass multiples of 64, so that
   contract is what actually needs to hold. Determine empirically on first run.)

Failure handling: set `Failed`, then under the print mutex dump: check name, variant
name, `len`, buffer offset, `counter64`, thread index, global `--seed`/`--threads`,
first differing byte index, and 16 hex bytes of expected vs actual around it. All other
workers observe `Failed` and exit; process returns 1.

Success output: `functional sweep passed: <N> iterations, <T> threads, <D>s, seed <S>`.

## 6. Mode 2: file format

Constants: `DataBlockSize = 4096`, `RecordSize = 32`, `RecordsPerGroup = 4096 / 32 =
128`. One **group** = one 4 KiB hash page followed by the 128 data blocks it describes:

```
offset 0                4096         4096 + 128*4096
| hash page (4 KiB) | block 0 | block 1 | ... | block 127 |     GroupSize = 4096 * 129 = 528384
```

File = one 4 KiB header page + `GroupCount` groups, nothing else:

```cpp
struct THashFileHeader {                  // occupies the first 4096 bytes, rest zero
    char   Magic[8];        // "YDBHASH1"
    ui32   Version;         // 1
    ui32   DataBlockSize;   // 4096
    ui32   RecordSize;      // 32
    ui32   RecordsPerGroup; // 128
    ui64   GroupCount;
    ui64   TotalDataBlocks; // GroupCount * RecordsPerGroup
    ui64   Seed;            // PRNG seed used for data generation
    ui64   HeaderHash;      // XXH3_64bits of the header bytes above (with this field zeroed)
};

struct TBlockRecord {                     // 32 bytes, little-endian (x86-64 only, just memcpy)
    ui64 Xxh3;              // XXH3_64bits(block, 4096)
    ui64 City64;            // CityHash_v1_0_2::CityHash64(block, 4096)
    ui64 ChaChaDigest;      // XXH3_64bits(ciphertext of block, 4096); see below
    ui64 BlockIndex;        // global data-block index; self-describing, catches layout bugs
};
static_assert(sizeof(TBlockRecord) == 32);
```

* **ChaChaDigest**: ciphertext of the block under the fixed key/zero IV with
  `counter64 = BlockIndex * (4096 / 64)` — i.e. exactly the keystream a single
  continuous encryption of the whole data area would use, but computed independently
  per block (parallelizable, no keystream reuse). Ciphertext goes to a per-thread 4 KiB
  scratch buffer; only its XXH3 digest is persisted (persisting ciphertext itself would
  double the file for no extra detection power).
* **Data generation** is deterministic: block `i` is filled by SplitMix64 seeded with
  `Seed ^ (i * 0xff51afd7ed558ccdULL)` (any fixed odd multiplier; document the exact
  formula in a comment — `--verify-data` on the reader re-derives it from the header's
  `Seed`). Two machines given the same seed and size produce **bit-identical files**.
* **Sizing** from `--output-size G` (GiB, total file size):
  `GroupCount = floor((G * 2^30 - 4096) / 528384)`, minimum 1. Print the effective
  file size and block count. Example: `--output-size 1` -> `GroupCount = 2032`,
  `TotalDataBlocks = 260096`, file size 1,073,680,384 bytes (~0.99995 GiB).

## 7. Writer (`--output`)

1. Create the file (POSIX `open(O_RDWR|O_CREAT|O_TRUNC, 0644)`), preallocate with
   `fallocate(fd, 0, 0, fileSize)` — real allocation, so ENOSPC surfaces now instead of
   as SIGBUS mid-write; fall back to `ftruncate` (+ a printed warning) if the
   filesystem returns EOPNOTSUPP. On macOS, skip `fallocate` entirely (`#if
   defined(_darwin_)`) and go straight to `ftruncate` — the syscall doesn't exist there
   at all, so ENOSPC on that platform surfaces lazily as SIGBUS mid-write (§2a). Then
   `mmap(PROT_READ|PROT_WRITE, MAP_SHARED)`. Wrap fd+mapping in a small RAII helper in
   `func.cpp`. (Plain POSIX is fine — this mode works on Linux and macOS, ARM64
   included; `util/system/filemap.h`'s `TFileMap` is an acceptable alternative.)
2. Thread 0 writes the header (HeaderHash computed last).
3. Partition **groups** contiguously: thread `t` of `N` owns groups
   `[t*GroupCount/N, (t+1)*GroupCount/N)`. Groups are 4 KiB-page-multiples and
   disjoint, so threads never touch the same page — no synchronization, no false
   sharing. Contiguous ranges keep writeback mostly sequential per thread.
4. Per data block, the owning thread: generates the data directly into the mapping,
   computes `Xxh3` / `City64` / `ChaChaDigest`, fills the record in the group's hash
   page. **Cross-variant check while writing**: compute each family with all supported
   variants (reuse the §4.3 tables) and fail the run on any disagreement — the write
   pass doubles as the goal-1 equivalence test over `G` GiB of unique data. The stored
   value is the reference variant's (they are all equal or we aborted).
5. Progress: an atomic group counter; the main thread prints every ~5 seconds
   (`groups done / total, MiB/s`). Main thread joins workers, then computes the
   **manifest digest**: XOR over all records of `XXH3_64bits(record, 32)` (records
   embed `BlockIndex`, so XOR's order-independence is safe; each thread returns its
   range's partial XOR, main thread combines). Print it:
   `manifest digest: 0x....` — two machines can compare runs by this single number
   without moving the file.
6. `msync(MS_SYNC)` + `munmap` + `close`. Return 0 only if no mismatches.

## 8. Verifier (`--input`)

1. `open(O_RDONLY)` + `mmap(PROT_READ, MAP_SHARED)`.
2. Validate the header: magic, version, `RecordSize == sizeof(TBlockRecord)`,
   `DataBlockSize`, `HeaderHash`, and `fileSize == 4096 + GroupCount * 528384`. Any
   failure => explanatory message, exit 1.
3. Same group partitioning across `--threads`. Per block, each thread:
   * checks `record.BlockIndex == globalIndex`;
   * recomputes `Xxh3` with **every** supported xxh3 variant (scalar, sse2, avx2,
     dispatch) — each must equal `record.Xxh3`;
   * recomputes `City64` — must equal `record.City64`;
   * for **every** supported chacha variant: encipher the stored block into scratch
     with `counter64 = BlockIndex * 64`, digest with XXH3, compare to
     `record.ChaChaDigest`;
   * with `--verify-data`: regenerate the block's PRNG bytes from the header `Seed`
     and memcmp against the stored block (distinguishes "data corrupted" from
     "hash implementation diverges" when investigating a failure).
4. Mismatch accounting: per-thread counters (mismatches by field+variant), details of
   the first 10 mismatches globally (block index, field, variant, expected, actual)
   under the print mutex. Do **not** stop on first mismatch — a corruption scan wants
   totals — but do stop early if mismatches exceed 1000 (the file is garbage; say so).
5. Final report: blocks verified, variants used on this machine, mismatch totals,
   manifest digest (computed the same way as the writer, so the two runs' digests can
   be compared even when the persisted records were never wrong). Exit 1 on any
   mismatch.

Cross-machine workflow this enables (document in README): machine A runs
`--functional --output f --output-size 8`, ships `f` (or just compares manifest
digests of two independently generated files with the same seed/size), machine B runs
`--functional --input f`. Any AVX-512/AVX2/SSE2/scalar divergence between the two
CPUs' implementations shows up as a specific field+variant mismatch at a specific
block.

## 9. `ya.make` changes

```
SRCS(
    ...existing...
    func.cpp
    variants.cpp
    kernel_xxh_scalar.cpp
)
```

`kernel_xxh_scalar.cpp` needs no special flags (scalar) and builds on every platform
except Windows (§2a) — same for the new `kernel_xxh_neon.cpp` on ARM64. No new
PEERDIRs: getopt, crypto, cityhash, xxHash are already there; mmap/fallocate come from
libc. The file I/O paths in `func.cpp` sit under `#if !defined(_win_)` (mmap works on
both Linux and macOS; only the `fallocate` call itself is further guarded by
`#if defined(_darwin_)` vs plain, per §2a/§7), with a stub that prints "file mode not
supported on this platform" on Windows. Also note the vendored xxHash library's
`ya.make` must exclude `xxh_x86dispatch.c` under `IF (NOT ARCH_ARM64)` (§2a) — without
that fix, the whole binary fails to compile on ARM64.

## 10. Implementation checklist

1. `kernel_xxh_scalar.cpp` (mirror sse2 TU, `XXH_VECTOR 0`) + registry entries in
   `kernels.h` / `bench.cpp::AllKernels`.
2. One-shot hash exports in the three pinned xxh TUs; declarations in `variants.h`.
3. `variants.h/.cpp` — `THashVariant` / `TCipherVariant` tables (§4.3).
4. `func.h/.cpp` — sweep (§5), file header/record structs (§6), writer (§7),
   verifier (§8), RAII mmap helper, thread pool with `Failed`/progress atomics.
5. `main.cpp` — `--functional` and the new options, validation (§3), dispatch to the
   three sub-modes; keep `--perf` behavior untouched.
6. `ya.make` (§9).
7. Verify: build; run `--functional --duration-s 2 --threads 4`; write+verify a
   `--output-size 0.05` file with 1 and 4 threads (same manifest digest all four ways:
   write x2, verify x2); flip one byte in the file with `dd` and confirm the verifier
   reports exactly one block's mismatches and exits 1; run `--input` on a truncated
   file and confirm the header/size validation rejects it cleanly.

## 11. Pitfalls recap

* Pinned XXH3 symbols are `static` (`XXH_INLINE_ALL`) — they *must* be re-exported via
  wrappers from inside their own TUs; never `#include "xxhash.h"` with different
  `XXH_VECTOR` values from shared headers.
* Nothing but trivial wrappers goes into the `-mavx2` TU; check `CachedHaveAVX512F()`
  before *constructing* `ChaCha512` (its SetKey/SetIV already execute AVX-512).
* Fresh cipher + explicit `SetIV(iv, &counter64)` per block in file mode; the
  per-block counter is `BlockIndex * 64` so keystreams never overlap and equal one
  continuous stream.
* `ChaCha512` has no `Decipher`; round-trip via double `Encipher` for all variants.
* Streaming-split segments may need to be multiples of 64 bytes (except the last) —
  test, and pin down whichever contract actually holds (§5.2 note).
* `fallocate` before `mmap` writes, or ENOSPC becomes SIGBUS.
* Threads own whole groups (page-aligned, disjoint) — never split a group between
  threads.
* Records are fixed 32-byte little-endian structs; `static_assert(sizeof == 32)`;
  `BlockIndex` inside the record is what makes the XOR manifest digest order-safe.
* Reproducibility: every random decision derives from `--seed` (+ thread index); print
  seed and thread count in every failure message.
* ARM64 (§2a): the vendored xxHash library's `xxh_x86dispatch.c` must be excluded via
  `IF (NOT ARCH_ARM64)` in its own `ya.make`, or nothing on that architecture compiles
  at all; `kernel_xxh_scalar.cpp` must **not** be grouped with the x86-only kernels, or
  the equivalence sweep silently runs zero xxh3 checks there; `fallocate()` must be
  skipped (not just "tried and fall back") under `#if defined(_darwin_)`, since it
  isn't declared on macOS at all and referencing it there is a compile error, not a
  runtime EOPNOTSUPP.
