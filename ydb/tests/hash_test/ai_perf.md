# Design: hash/cipher bandwidth benchmark (`ydb/tests/hash_test`)

This document is a complete implementation spec for the performance test described in
`README.md`. It is written so that an implementer (human or LLM) can build it without
re-doing the research. All referenced APIs were verified against this repo.

## 1. Goal

Measure **bandwidth in GB/s** (bytes processed / second, GB = 1e9 bytes) of:

| ID              | What                          | Implementation used                                        |
|-----------------|-------------------------------|------------------------------------------------------------|
| `mem-read`      | pure memory read baseline     | hand-written scalar ui64 read loop (8 accumulators)        |
| `mem-read-avx2` | pure memory read, vector      | hand-written AVX2 32-byte load loop (optional but wanted)  |
| `chacha-int`    | ChaCha, integer/scalar        | `ChaCha` from `ydb/core/blobstorage/crypto/chacha.h`       |
| `chacha-sse`    | ChaCha, SSE                   | `ChaChaVec` from `ydb/core/blobstorage/crypto/chacha_vec.h`|
| `chacha-avx512` | ChaCha, AVX-512               | `ChaCha512` from `ydb/core/blobstorage/crypto/chacha_512/chacha_512.h` |
| `xxh3-scalar`   | XXH3_64bits, scalar code path | vendored `ydb/tests/hash_test/xxHash`, compile-time pinned |
| `xxh3-sse2`     | XXH3_64bits, SSE2 code path   | vendored `ydb/tests/hash_test/xxHash`, compile-time pinned |
| `xxh3-avx2`     | XXH3_64bits, AVX2 code path   | vendored `ydb/tests/hash_test/xxHash`, compile-time pinned |
| `city64`        | CityHash64                    | `CityHash_v1_0_2::CityHash64` from `contrib/restricted/cityhash-1.0.2/city.h` |

for every combination of:

* **working-set location**: L1, L2, L3, RAM (sizes configurable via CLI, defaults
  L1=48 KiB, L2=1280 KiB, L3=48 MiB);
* **thread placement scenario**: single core, two/N distinct physical cores, and
  hyper-threaded siblings (two threads sharing one physical core), to expose how badly
  vector-heavy kernels degrade under SMT compared to integer kernels.

Additionally, a **mixed-kernel HT scenario** (§9a) runs *different* kernels on the two
SMT siblings of one physical core — integer implementation as the "victim", a vector
implementation as the "aggressor" — to measure how much SSE/AVX execution on one
hyper-thread degrades the integer sibling (and vice versa). Reported in its own table
(§12a).

## 2. Non-goals

* No latency measurement (only throughput / bandwidth).
* No small-input hashing (block size is >= 1 KiB; per-call fixed overhead is amortized).
* `--perf` itself stays Linux/x86-64-only: it relies on `TAffinity`
  (`sched_setaffinity`) for core pinning and on the SSE/AVX/AVX-512 kernels for
  its whole point (integer-vs-vector, HT contention). `main.cpp` refuses
  `--perf` outright on Windows or ARM64 (`#if defined(_win_) ||
  defined(_arm64_)`) rather than trying to run a degraded version.
* The kernel set itself is portable, though: kernels with no ISA dependency
  (`mem-read`, `chacha-int`, `xxh3-scalar`, `city64`) and the ARM-specific
  `xxh3-neon` kernel (XXH3 pinned to `XXH_VECTOR 4`/NEON, NEON being baseline
  on AArch64 just like SSE2 is on x86-64) all build and run on ARM64 too --
  this is what `--functional` (ai_func.md) exercises there, since `--perf`
  itself is unavailable on that architecture.

## 3. CLI

Use `NLastGetopt::TOpts` (`library/cpp/getopt`) + `StringSplitter` from
`util/string/split.h` for the core lists.

```
./hash_test --perf
    [--l1 48]              # L1d size per core, KiB
    [--l2 1280]            # L2 size per core, KiB
    [--l3 49152]           # L3 size (shared), KiB
    [--block-size 4096]    # bytes fed to one hash/encipher call
    --left-cores=1,2,3     # physical cores (one logical CPU per physical core)
    --right-cores=33,34,35 # HT siblings: right-cores[i] must be the SMT sibling of left-cores[i]
    [--run-ms 500]         # duration of one timed sample
    [--samples 3]          # samples per cell; median is reported
    [--filter substr]      # run only kernels whose name contains substr
    [--csv]                # machine-readable output in addition to the table
```

Validation:

* `--left-cores` is required for `--perf`; `--right-cores` is required only for the HT
  scenarios (skip them with a notice if absent).
* If `--right-cores` is given, it must have the same length as `--left-cores`.
* Optional but recommended: verify sibling relationship by reading
  `/sys/devices/system/cpu/cpu<N>/topology/thread_siblings_list` for each left core and
  checking the paired right core is listed. Print a warning (do not abort) on mismatch,
  since sysfs may be unavailable in containers.
* Block size must be a power of two, >= 1024. Working sets are rounded **down** to a
  multiple of block size so the inner loop needs no tail handling.

Without `--perf` the binary keeps doing what it does now (returns 0) — leave room for
future correctness tests.

## 4. File layout

```
ydb/tests/hash_test/
  main.cpp               # CLI parsing, scenario orchestration, output
  bench.h / bench.cpp    # harness: kernel registry, thread runner, timing, barriers
  kernels.h              # TKernelDesc, kernel fn signature, RunLoop<T> template helper
  kernel_mem.cpp         # mem-read (scalar)
  kernel_mem_avx2.cpp    # mem-read-avx2                     [compiled with -mavx2]
  kernel_chacha.cpp      # chacha-int
  kernel_chacha_vec.cpp  # chacha-sse
  kernel_chacha512.cpp   # chacha-avx512  (no special flags needed, see 7.3)
  kernel_xxh_scalar.cpp  # xxh3-scalar (XXH_VECTOR 0; integer reference for mixed-HT)
  kernel_xxh_sse2.cpp    # xxh3-sse2                         [x86-64 only]
  kernel_xxh_avx2.cpp    # xxh3-avx2                         [x86-64 only, compiled with -mavx2]
  kernel_xxh_neon.cpp    # xxh3-neon (XXH_VECTOR 4)          [ARM64 only]
  kernel_city.cpp        # city64
```

Rationale for one TU per kernel: (a) per-file ISA flags in ya.make, (b) each hot loop is
statically compiled and fully inlinable inside its TU, (c) no accidental cross-kernel
inlining/hoisting.

## 5. Kernel interface — dispatch without hot-path indirection

**Key principle**: the *only* indirect call is one call per "chunk" of work (~100 µs of
work, see §8). Everything inside a chunk — the pass loop, the block loop, the hash
function itself — is direct, statically compiled, inlinable code inside the kernel's TU.
At 4 KiB blocks and ~100 µs chunks, one indirect call amortizes over tens of thousands
of hash calls; its cost is unmeasurable.

`kernels.h`:

```cpp
#pragma once
#include <util/system/types.h>
#include <util/system/compiler.h>

// Runs `passes` full passes over the working set [data, data + wsSize),
// processing it in blockSize-byte blocks. wsSize is guaranteed to be a
// non-zero multiple of blockSize. Returns an accumulator that the harness
// stores into a volatile sink (anti-dead-code-elimination).
// ctx is per-thread kernel state (e.g. a cipher object), never shared.
using TKernelFn = ui64 (*)(ui8* data, size_t wsSize, size_t blockSize,
                           ui64 passes, void* ctx);

struct TKernelDesc {
    const char* Name;               // e.g. "chacha-sse"
    bool (*IsSupported)();          // CPU feature gate; nullptr => always supported
    void* (*CreateCtx)();           // per-thread state; nullptr => no state
    void (*DestroyCtx)(void* ctx);
    TKernelFn Run;
};

// Shared loop skeleton. TOp::ProcessBlock must be Y_FORCE_INLINE.
// Instantiated separately in each kernel TU => the whole nest compiles to
// one flat, ISA-specific loop with zero indirection inside.
template <class TOp>
ui64 RunLoop(ui8* data, size_t wsSize, size_t blockSize, ui64 passes, void* ctx) {
    TOp& op = *static_cast<TOp*>(ctx);
    ui64 acc = 0;
    for (ui64 p = 0; p < passes; ++p) {
        for (size_t off = 0; off < wsSize; off += blockSize) {
            acc ^= op.ProcessBlock(data + off, blockSize);
        }
    }
    return acc;
}
```

The registry is a plain array in `bench.cpp`:

```cpp
extern const TKernelDesc KernelMemRead;      // defined in kernel_mem.cpp
extern const TKernelDesc KernelMemReadAvx2;  // kernel_mem_avx2.cpp
// ... etc ...
static const TKernelDesc* const Kernels[] = { &KernelMemRead, ... };
```

Anti-DCE rules:

* **Hashes / memory reads**: fold every per-block result into `acc` (XOR). The harness
  writes the final value into a file-scope `volatile ui64 Sink;`. This is the same
  "result flows into observable data" technique the xxHash bench harness uses
  (no `volatile` in the hot loop itself).
* **Ciphers**: they write ciphertext through a pointer, which is already observable;
  `ProcessBlock` just returns 0. Do not add extra reads of the output.

## 6. Kernel implementations

### 6.1 `mem-read` (kernel_mem.cpp)

Scalar baseline: read the block as `ui64` with **8 independent accumulators** (breaks the
dependency chain so the loop is load-bound, not add-latency-bound), XOR-combine at the end.

```cpp
struct TMemReadOp {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        const ui64* w = reinterpret_cast<const ui64*>(p);
        size_t n = len / 8;
        ui64 a0=0,a1=0,a2=0,a3=0,a4=0,a5=0,a6=0,a7=0;
        for (size_t i = 0; i + 8 <= n; i += 8) {
            a0 ^= w[i+0]; a1 ^= w[i+1]; a2 ^= w[i+2]; a3 ^= w[i+3];
            a4 ^= w[i+4]; a5 ^= w[i+5]; a6 ^= w[i+6]; a7 ^= w[i+7];
        }
        return a0^a1^a2^a3^a4^a5^a6^a7;
    }
};
ui64 RunMemRead(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TMemReadOp>(d, ws, bs, p, ctx);
}
static void* CreateMemCtx() { return new TMemReadOp(); }
static void DestroyMemCtx(void* c) { delete static_cast<TMemReadOp*>(c); }
const TKernelDesc KernelMemRead{"mem-read", nullptr, CreateMemCtx, DestroyMemCtx, RunMemRead};
```

Note: block size is a power of two >= 1024, so the `i + 8 <= n` unroll never leaves a tail.

### 6.2 `mem-read-avx2` (kernel_mem_avx2.cpp)

Same structure with `__m256i` loads (`_mm256_load_si256`, buffers are 4 KiB-aligned, see
§9) and 8 vector accumulators, `_mm256_xor_si256` folding, final horizontal XOR via two
`_mm256_extracti128_si256` + scalar. Gate: `IsSupported = NX86::CachedHaveAVX2` (from
`util/system/cpu_id.h`). This gives the true "speed of light" for each cache level so the
hash numbers have a meaningful ceiling.

### 6.3 `chacha-int`, `chacha-sse`, `chacha-avx512`

All three classes share an identical API (verified): `KEY_SIZE = 32`, 8-byte IV,
`SetKey(key, 32)`, `SetIV(iv)`, `Encipher(src, dst, size)`, default 8 rounds. Usage
pattern per thread:

```cpp
struct TChaChaVecOp {
    ChaChaVec Cipher;                      // 8 rounds default — keep it
    TChaChaVecOp() {
        alignas(16) static const ui8 Key[32] = { /* any fixed bytes */ };
        static const ui8 Iv[8] = {};
        Cipher.SetKey(Key, 32);
        Cipher.SetIV(Iv);
    }
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        Cipher.Encipher(p, p, len);        // in-place, see below
        return 0;
    }
};
```

* **In-place encryption** (`src == dst`): safe for all three — they generate keystream
  and XOR it into the output; loads happen before stores within each block. This keeps
  the working set equal to the buffer size (an out-of-place scheme would double the
  cache footprint and muddy the L1/L2 columns). Add a one-time startup sanity check
  (see §11) to be safe.
* Do **not** call `SetIV` per block. Each `Encipher` call continues the keystream from
  internal state; that is the realistic streaming mode and avoids polluting the
  measurement with re-keying.
* Keystream position never overflows (64-bit block counter).
* `ChaCha512` gate: `IsSupported = NX86::CachedHaveAVX512F`. Important: `CreateCtx` also
  executes AVX-512 code (SetKey/SetIV touch `vec512` state), so the harness must check
  `IsSupported` **before** calling `CreateCtx`, not just before `Run`.
* `ChaChaVec` picks its aligned fast path at runtime from pointer alignment — our
  buffers are 4 KiB-aligned and block size is a multiple of 64, so every block pointer
  is 16-byte aligned and we always hit the `Aligned=true` path. State this in a comment.

`Encipher` is an out-of-line call into the crypto library for all three variants — fine,
one call per 4 KiB block is the same cost for all cipher variants and is realistic usage.

### 6.4 `xxh3-sse2` / `xxh3-avx2`

The runtime dispatcher (`xxh_x86dispatch.h`) **cannot** force a lower ISA on a capable
CPU — it always picks the best via CPUID. To pin the code path, compile two private
copies of XXH3 with `XXH_INLINE_ALL` (everything becomes `static`, so no symbol clashes
with the vendored library or each other):

`kernel_xxh_sse2.cpp`:

```cpp
#define XXH_INLINE_ALL
#define XXH_VECTOR 1   /* XXH_SSE2; numeric value — the named macro is defined later in the header */
#include "xxHash/xxhash.h"
#include "kernels.h"

struct TXxhOp {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        return XXH3_64bits(p, len);
    }
};
// ... RunLoop<TXxhOp> wrapper + TKernelDesc{"xxh3-sse2", nullptr /*SSE2 is x86-64 baseline*/, ...}
```

`kernel_xxh_avx2.cpp`: identical but `#define XXH_VECTOR 2 /* XXH_AVX2 */`, compiled with
AVX2 flags (§7.3), `IsSupported = NX86::CachedHaveAVX2`.

`kernel_xxh_scalar.cpp`: identical but `#define XXH_VECTOR 0 /* XXH_SCALAR */`, no
special flags, no gate. This is the xxh3 family's integer implementation — required as
the "victim" side of the mixed-HT scenario (§9a), and useful as a scalar row in the
main tables. Builds on every platform (including ARM64) since it has no ISA dependency.

`kernel_xxh_neon.cpp` (ARM64 only): identical pattern with `#define XXH_VECTOR 4 /*
XXH_NEON */`, kernel name `xxh3-neon`, no special compile flags and no `IsSupported`
gate (NEON is baseline on AArch64, same reasoning as SSE2 being baseline on x86-64).
This is ARM's analogue of `xxh3-sse2`/`xxh3-avx2` — xxHash has no AVX2-equivalent
"second tier" vector width on ARM (SVE exists in xxHash but isn't available on Apple
Silicon), so NEON is the only vector kernel on that architecture.

**Warning to implementer**: keep the AVX2 TUs (`kernel_xxh_avx2.cpp`,
`kernel_mem_avx2.cpp`) minimal — nothing but the kernel. The whole TU is compiled with
`-mavx2`, so any code in it may emit AVX2 instructions; it must never run before the
runtime gate passes.

Since we compile pinned variants ourselves, `main.cpp` should drop
`#include "xxHash/xxh_x86dispatch.h"`. Keep the `PEERDIR` on
`ydb/tests/hash_test/xxHash` — it provides the `ADDINCL GLOBAL` for the header. Optionally
call `XXH3_64bits_dispatch` once at startup to cross-check both pinned variants return
the same hash (see §11).

### 6.5 `city64`

```cpp
Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
    return CityHash_v1_0_2::CityHash64(reinterpret_cast<const char*>(p), len);
}
```

Out-of-line call into contrib — fine (same reasoning as ciphers). No feature gate.

## 7. Build (`ya.make`)

Replace the current module-wide `CFLAGS(-mavx512f)` — it is wrong to compile every TU
with AVX-512 (the compiler may auto-vectorize harness code with instructions that crash
on non-AVX-512 hosts before any runtime gate runs).

```
PROGRAM()

PEERDIR(
    ydb/tests/hash_test/xxHash
    ydb/core/blobstorage/crypto
    contrib/restricted/cityhash-1.0.2
    library/cpp/getopt
    ydb/library/actors/util          # TAffinity
)

SRCS(
    main.cpp
    bench.cpp
    kernel_mem.cpp
    kernel_chacha.cpp
    kernel_chacha_vec.cpp
    kernel_chacha512.cpp
    kernel_xxh_scalar.cpp
    kernel_xxh_sse2.cpp
    kernel_city.cpp
)

SRC(kernel_mem_avx2.cpp -mavx2)
SRC(kernel_xxh_avx2.cpp -mavx2)

END()
```

Notes:

1. `SRC(file, flags...)` compiles a single file with extra flags (defined in
   `build/ymake.core.conf`; see real usage in `library/cpp/dot_product/ya.make`).
   `SRC_C_AVX2(file)` is an alternative that adds `-mavx2 -mfma -mbmi -mbmi2`; plain
   `SRC(file -mavx2)` is preferred here to keep the ISA delta exactly "AVX2".
2. `kernel_chacha512.cpp` needs **no** special flags: `ChaCha512::Encipher` is an
   out-of-line function inside `ydb/core/blobstorage/crypto/chacha_512`, which is already
   built with `-mavx512f` by its own ya.make. Our TU only calls it. Including
   `chacha_512.h` without `-mavx512f` is fine (the header only declares types).
3. `kernel_xxh_scalar.cpp` and `kernel_city.cpp` have no ISA dependency and build
   everywhere except Windows; the x86 SIMD kernels (`kernel_mem_avx2.cpp`,
   `kernel_chacha_vec.cpp`, `kernel_chacha512.cpp`, `kernel_xxh_sse2.cpp`,
   `kernel_xxh_avx2.cpp`) stay under `IF (NOT OS_WINDOWS AND NOT ARCH_ARM64)`;
   `kernel_xxh_neon.cpp` gets its own `IF (NOT OS_WINDOWS AND ARCH_ARM64)` block.
   `kernels.h`/`bench.cpp`'s `AllKernels[]`/`variants.h`/`variants.cpp` mirror the same
   three-way split with matching `#if` guards.
4. The vendored `ydb/tests/hash_test/xxHash` library's `xxh_x86dispatch.c` hard
   `#error`s on any non-x86 target -- its own `ya.make` must exclude it under
   `IF (NOT ARCH_ARM64)`, or the whole binary fails to compile on ARM64 regardless of
   anything else described here.

## 8. Measurement methodology

Modeled on xxHash's `tests/bench` (calibrated loop counts, best/median of several
samples, result-as-data anti-DCE), adapted for multithreading.

Per (scenario × kernel × working-set level) cell:

1. **Setup** (per thread, on the pinned thread — see §9): allocate + fill buffer,
   `CreateCtx()`.
2. **Warmup + calibration**: run the kernel untimed for ~100 ms (loop
   `Run(data, ws, bs, 1, ctx)` and accumulate elapsed via `THPTimer` until 100 ms).
   This pages in and warms the buffer and measures a rough passes/sec rate. From it
   compute `chunkPasses = max<ui64>(1, rate * 100us)` — the number of passes that takes
   ~100 µs. All subsequent timed work calls `Run(..., chunkPasses, ...)`, so the
   stop-flag check and the indirect call happen only every ~100 µs.
3. **Timed sample** (repeated `--samples` times, default 3):
   * Main thread resets a `std::atomic<bool> Stop{false}` and an atomic spin barrier.
   * Each worker: arrives at the barrier (spin on `std::atomic<ui32>` with
     `SpinLockPause()` from `util/system/spinlock.h` — no futex sleep, so all threads
     start truly simultaneously), then:
     ```cpp
     THPTimer timer;
     ui64 chunks = 0;
     while (!Stop.load(std::memory_order_relaxed)) {
         Sink ^= Run(data, ws, bs, chunkPasses, ctx);   // Sink write is per-thread, see §9
         ++chunks;
     }
     double seconds = timer.Passed();
     // bytes = chunks * chunkPasses * ws
     ```
   * Main thread sleeps `--run-ms` (default 500 ms) after releasing the barrier, then
     sets `Stop`, joins the sample.
   * Per-thread bandwidth = `bytes / seconds / 1e9` GB/s. Sample aggregate = **sum of
     per-thread bandwidths** (threads run concurrently over the same window; each is
     normalized by its own elapsed time, so stragglers do not skew the result).
4. **Cell result**: the **median** aggregate across samples (median, not best-of, because
   in multithreaded runs "best" can combine mutually inconsistent thread states; median
   is robust to a one-off noisy sample). Also keep min/max internally for a `--csv` column.

Rounding rule: `ws` is rounded down to a multiple of `blockSize` (already guaranteed by
§10 sizing since all defaults are powers of two / multiples).

Timing primitives: `THPTimer` (`util/system/hp_timer.h`) is sufficient — resolution is
based on TSC and samples are >= 100 ms. Do not use raw `GetCycleCount()` arithmetic; no
need, and THPTimer already handles frequency.

## 9. Threading, affinity, false sharing, NUMA

* **Thread creation**: `std::thread` (or `TThread`) per worker. First thing inside the
  worker: pin to exactly one logical CPU using `TAffinity` from
  `ydb/library/actors/util/affinity.h`:
  ```cpp
  ui8 mask[MaxCpu] = {};   // MaxCpu = max core id + 1
  mask[cpuId] = 1;
  TAffinity(mask, MaxCpu).Set();
  ```
  (It wraps `sched_setaffinity` for the current thread.)
* **Buffer allocation on the pinned thread** (after `Set()`): allocate with
  `new ui8[size + 4096]` and align the pointer up to 4096 with `AlignUp` from
  `util/system/align.h` (same idiom as `TAlignedBuf` in
  `ydb/core/blobstorage/crypto/ut/ut_helpers.h`). Then **fill it from that thread**
  (first-touch => NUMA-local pages). Fill with a cheap PRNG (e.g. multiply-shift like
  xxHash's `initBuffer`) so the content is non-trivial.
* **False-sharing avoidance**:
  * Each worker gets its own `TWorkerSlot` in a pre-allocated array, with
    `alignas(128)` and padding to 128 bytes (two cache lines — covers the adjacent-line
    prefetcher). The slot holds everything the worker writes: its `Sink`, `chunks`,
    `seconds`, pointers to its buffer/ctx.
    ```cpp
    struct alignas(128) TWorkerSlot {
        ui8* Data = nullptr;
        void* Ctx = nullptr;
        ui64 Sink = 0;
        ui64 Chunks = 0;
        double Seconds = 0;
        char Pad[128 - 40];   // adjust so sizeof == 128 (static_assert it)
    };
    static_assert(sizeof(TWorkerSlot) == 128);
    ```
  * The `Stop` flag and the barrier counter are each on their own `alignas(128)` line.
    They are read-shared during the run (fine) and written once (start/stop).
  * Buffers are per-thread and >= 4 KiB apart by construction; no sharing.
* **Thread reuse**: create the worker threads **once per scenario** and feed them cells
  through a simple job structure (kernel ptr, ws, chunkPasses, barrier generation), so
  buffers can also be reused across kernels within a (scenario, level) pair — allocate
  the max ws per level once per thread. This avoids tens of thousands of thread spawns
  and keeps NUMA placement stable. (Simplest correct alternative: spawn threads per
  cell. Acceptable if it keeps the code much simpler; warmup hides page-fault costs.
  Implementer's choice; prefer reuse.)

### Scenarios

Given `left = {l0, l1, ..., ln-1}` and `right = {r0, ..., rn-1}` (ri = SMT sibling of li):

| Scenario   | Threads on cores                | Requires        | Purpose                          |
|------------|---------------------------------|-----------------|----------------------------------|
| `1T`       | {l0}                            | 1 left core     | raw single-core baseline         |
| `2T-phys`  | {l0, l1}                        | 2 left cores    | two independent physical cores   |
| `2T-ht`    | {l0, r0}                        | right list      | two threads on one physical core |
| `NT-phys`  | all left cores                  | n > 2           | scaling across physical cores    |
| `2NT-ht`   | all left + all right cores      | right list, n>1 | full SMT scaling                 |

Skip a scenario (with a printed notice) when its requirements are not met.
The interesting README comparison is `2T-phys` vs `2T-ht`: per-thread bandwidth of
vector kernels should crater under `2T-ht` while integer kernels degrade less.

## 9a. Mixed-kernel HT scenario (`mixed-ht`)

All scenarios above are *homogeneous*: every thread runs the same kernel. This
scenario answers a different question: **how much does a vector implementation running
on one hyper-thread slow down an integer implementation on the SMT sibling** (and vice
versa)? SMT siblings share execution ports; a sibling saturating the vector ports can
stall an integer neighbor far more than another integer thread would.

### Placement and pairs

Runs on `{l0, r0}` (same placement as `2T-ht`); requires `--right-cores`. Thread 0
("victim", integer implementation) on `l0`, thread 1 ("aggressor", vector
implementation) on `r0`. Fixed pair table (data-driven array in `bench.cpp`, so pairs
are trivial to add):

| Pair | Victim (integer) | Aggressor (vector) |
|------|------------------|--------------------|
| 1    | `chacha-int`     | `chacha-sse`       |
| 2    | `chacha-int`     | `chacha-avx512`    |
| 3    | `xxh3-scalar`    | `xxh3-sse2`        |
| 4    | `xxh3-scalar`    | `xxh3-avx2`        |

(`xxh3-scalar` is the pinned `XXH_VECTOR 0` build from §6.4 — the xxh3 family has no
other integer implementation, which is why that kernel exists.) Skip a pair with a
notice if either kernel is unsupported on this CPU. `--filter` applies to pairs too:
keep a pair if *either* kernel name contains the filter substring.

### Harness changes (per-thread kernels)

`TScenarioRunner` currently assumes one `JobKernel` and one `JobChunkPasses` for all
workers. Generalize minimally:

* Job carries a **per-thread kernel** array (`JobKernels[threadIdx]`); homogeneous
  cells simply fill it with the same pointer — existing behavior and results unchanged.
* `chunkPasses` becomes **per-slot** (`slot.ChunkPasses`): each worker calibrates its
  *own* kernel in `WarmupAndCalibrate` (kernels differ by >10x in rate, so a shared
  value is wrong). The bytes math in `RunCell` reads `slot.ChunkPasses` instead of the
  shared `JobChunkPasses`. Calibration already runs concurrently on both siblings,
  which is exactly the contention regime we want to calibrate under.
* Each worker keeps its own ctx for its own kernel; ctx invalidation on kernel change
  is already per-worker.

### Baselines and measurement

Interpretation needs a same-working-set solo baseline (the `1T` table won't do: it
uses full-cache ws sizes, while `mixed-ht` halves L1/L2 like `2T-ht`). Per level:

1. **Solo baselines**: for each kernel appearing in any pair, run it alone on `l0`
   with the mixed scenario's ws (a 1-thread runner on `{l0}`; the sibling stays idle).
   Cache by (kernel, level) so each baseline is measured once, not once per pair.
2. **Mixed cell**: both threads run their respective kernels simultaneously (same
   barrier/Stop protocol as any cell). Record each thread's own GB/s separately —
   do **not** sum them; the whole point is the per-side degradation.
3. Per side, report median-of-samples GB/s and **retention** = mixed / solo for the
   same (kernel, level).

Working sets: identical formula to `2T-ht` (`SharesPhysicalCore = true`, 2 threads):
L1/L2 halved, L3 divided by 2, RAM = 4*L3. Both threads use the same ws — the two
kernels' footprints must not differ, or the cache columns stop being comparable.

Runtime cost with defaults: 4 pairs x 4 levels x (samples x run-ms) for mixed cells
plus <= 6 distinct kernels x 4 levels for baselines — roughly 1.5 minutes; acceptable.
Order the run so all baselines for a level are measured first, then the mixed cells.

## 10. Working-set sizing per cache level

A thread's buffer must fit in the *share of the cache the thread actually gets*:

* Residency factor: use **1/2** of the nominal capacity of the target level (constant
  `ResidencyFactor = 0.5`), leaving room for stacks/ctx/code so the set truly resides.
* Cache sharing divisor per scenario:
  * L1 and L2 are private per **physical core** → divide by 2 iff the scenario places 2
    threads on the same physical core (`2T-ht`, `2NT-ht`), else 1.
  * L3 is shared by **all** threads → divide by the total thread count.
  * RAM: no divisor; per-thread ws = `4 * l3` (defaults: 192 MiB/thread), guaranteeing
    the aggregate footprint dwarfs L3.

```
wsPerThread(level, scenario) =
    level == RAM ? 4 * L3
                 : AlignDown(levelSize * 1/2 / sharingDivisor, blockSize)
```

With defaults and blockSize=4096: L1 → 20 KiB (1T) / 8 KiB (HT; note 24 KiB isn't a
multiple of 4096, hence AlignDown; print the actual ws used), L2 → 640 KiB / 320 KiB,
L3(1T) → 24 MiB, L3(2T) → 12 MiB, etc. **Print the effective per-thread ws in the
output header of every table** so results are interpretable.

Edge check: if `wsPerThread < blockSize`, clamp ws to blockSize and print a warning
(happens e.g. with `--block-size 65536` on the HT L1 cell).

## 11. Startup self-checks (cheap, always run before `--perf`)

1. Print detected CPU features (`NX86::CachedHaveSSE2/AVX2/AVX512F`) and which kernels
   will be skipped.
2. **Cipher cross-check**: encrypt a 64 KiB pattern buffer with `ChaCha`, `ChaChaVec`,
   and (if supported) `ChaCha512` — same key/IV, fresh objects, out-of-place — and
   `memcmp` the three ciphertexts. Then verify in-place `Encipher(p, p, len)` on a copy
   produces the same bytes (validates the in-place assumption of §6.3). Abort on
   mismatch: a perf number for a broken kernel is worse than no number.
3. **Hash cross-check**: `xxh3-sse2` and `xxh3-avx2` (if supported) must return equal
   hashes for the pattern buffer, and equal to `XXH3_64bits_dispatch` from the vendored
   library.

## 12. Output

Human-readable: one table per scenario. Rows = kernels, columns = levels, cells =
aggregate GB/s (sum over threads). Below each table print the per-thread ws line.

```
=== Scenario 2T-ht: cores {1, 33} (2 threads, SMT siblings) ===
per-thread working set: L1=8 KiB  L2=320 KiB  L3=12 MiB  RAM=192 MiB
kernel            L1        L2        L3       RAM
mem-read       123.4      98.7      45.6      18.2
mem-read-avx2  456.7     321.0      78.9      19.1
chacha-int       3.2       3.1       3.0       2.9
chacha-sse       9.8       9.5       8.9       7.2
chacha-avx512   21.3      20.1      15.4       9.8
xxh3-sse2       28.4      26.0      20.1      12.3
xxh3-avx2       55.1      50.2      31.7      13.0
city64          14.9      14.2      12.8      10.4
```

(Numbers above are illustrative placeholders.) With `--csv`, additionally emit lines:
`scenario,kernel,level,threads,ws_per_thread_bytes,block_size,gbps_median,gbps_min,gbps_max,gbps_per_thread_avg`.

Flush after each cell (`Cout.Flush()`), so partial results survive interruption — the
full matrix takes minutes (scenarios × kernels × 4 levels × samples × run-ms).

## 12a. Mixed-kernel HT output (separate table)

The mixed scenario (§9a) gets its **own table** after the homogeneous scenarios, so
the numbers don't have to be fished out of the main matrix. Two rows per pair — one
per side — each cell showing the side's own GB/s under contention and its retention
versus the solo baseline at the same working set:

```
=== Mixed HT scenario: cores {1, 33} (victim l0 + aggressor r0, SMT siblings) ===
per-thread working set: L1=8 KiB  L2=320 KiB  L3=12 MiB  RAM=192 MiB
pair / side                          L1              L2              L3             RAM
chacha-int   (+ chacha-sse)     3.1 ( 91%)      3.0 ( 90%)      2.9 ( 92%)      2.8 ( 95%)
chacha-sse   (+ chacha-int)     8.9 ( 87%)      8.6 ( 86%)      8.1 ( 88%)      6.9 ( 93%)
chacha-int   (+ chacha-avx512)  2.6 ( 76%)      2.5 ( 75%)      2.5 ( 79%)      2.7 ( 92%)
chacha-avx512(+ chacha-int)    19.8 ( 90%)     18.7 ( 89%)     14.2 ( 90%)      9.4 ( 95%)
xxh3-scalar  (+ xxh3-sse2)      ...
xxh3-sse2    (+ xxh3-scalar)    ...
xxh3-scalar  (+ xxh3-avx2)      ...
xxh3-avx2    (+ xxh3-scalar)    ...
```

(Illustrative numbers.) The row label is `<this side's kernel> (+ <sibling's kernel>)`;
retention % = this side's mixed GB/s divided by its solo GB/s at the same ws. Low
retention on a `chacha-int`/`xxh3-scalar` row with an AVX aggressor is precisely the
effect the README hypothesizes.

With `--csv`, emit one line per side per level, distinguishable from the homogeneous
lines by the scenario name:
`mixed-ht,<kernel>,<level>,2,<ws>,<block>,<gbps_median>,<gbps_min>,<gbps_max>,<sibling_kernel>,<solo_gbps>,<retention>`
(three extra trailing columns; homogeneous lines keep their existing format).

## 13. Implementation checklist

1. `kernels.h` — interface + `RunLoop` template (§5).
2. Kernel TUs (§6), each ~30-50 lines: op struct, `RunLoop` instantiation, `TKernelDesc`.
3. `bench.h/bench.cpp` — registry array; `TWorkerSlot`; spin barrier; warmup/calibrate;
   sample loop; median aggregation (§8, §9); per-thread kernels + per-slot chunkPasses,
   pair table, solo-baseline cache, mixed-ht driver (§9a).
4. `main.cpp` — CLI (§3), self-checks (§11), scenario loop, tables (§12, §12a). Keep
   the existing platform `#if` guards.
5. `ya.make` — per §7. Remove the global `CFLAGS(-mavx512f)`.
6. Build: `./ya make --build relwithdebinfo ydb/tests/hash_test` and smoke-run with
   `--run-ms 50 --samples 1` on a couple of cores.

## 14. Pitfalls recap (do not skip)

* No module-wide `-mavx512f`/`-mavx2`; per-file `SRC()` only; keep ISA-flagged TUs
  minimal; gate with `NX86::CachedHave*` **before** `CreateCtx`, not only before `Run`.
* xxHash runtime dispatcher can't force a lower ISA — must pin via `XXH_VECTOR` +
  `XXH_INLINE_ALL` in dedicated TUs; use numeric `XXH_VECTOR` values (1=SSE2, 2=AVX2).
* Indirect calls only at chunk granularity (~100 µs), never per block.
* Anti-DCE: hash results XOR-folded and stored to a sink; never `volatile` inside loops.
* Per-worker state in `alignas(128)`, size-128 slots; `Stop`/barrier on their own lines.
* Buffers: 4 KiB-aligned, allocated and first-touched on the pinned thread.
* ChaCha: in-place, no per-block `SetIV`, 8 rounds everywhere (comparability), verified
  by the §11 cross-check.
* Working sets rounded down to a block-size multiple; effective sizes printed.
* Median of samples, per-thread-normalized bandwidth summed for aggregates.
* Mixed-ht: chunkPasses is per-slot (kernels differ >10x in rate — never share it);
  per-side GB/s reported separately, never summed; solo baselines must use the mixed
  scenario's (halved) working sets, not the 1T scenario's.
