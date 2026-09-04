#pragma once

#include <util/system/types.h>
#include <util/system/compiler.h>

#if defined(__clang__) || defined(__GNUC__)
#define Y_KERNEL_EXPORT __attribute__((used))
#else
#define Y_KERNEL_EXPORT
#endif

// Runs `passes` full passes over the working set [data, data + wsSize),
// processing it in blockSize-byte blocks. wsSize is guaranteed to be a
// non-zero multiple of blockSize. Returns an accumulator that the harness
// XORs into a per-thread sink (anti-dead-code-elimination): the result is
// never discarded, only ever combined into observable state, so the call
// itself (made through this function pointer) can't be optimized away even
// though the sink field is not declared volatile.
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

extern const TKernelDesc KernelMemRead;
#if !(defined(_win_) || defined(_arm64_))
extern const TKernelDesc KernelMemReadAvx2;
#endif
extern const TKernelDesc KernelChaChaInt;
#if !(defined(_win_) || defined(_arm64_))
extern const TKernelDesc KernelChaChaSse;
extern const TKernelDesc KernelChaChaAvx512;
extern const TKernelDesc KernelXxhSse2;
extern const TKernelDesc KernelXxhAvx2;
#endif
// xxh3-scalar is pure C with no ISA dependency, so it builds everywhere
// except Windows (kept out of Windows only to match the rest of the xxh3
// family's build grouping, not for any technical reason).
#if !defined(_win_)
extern const TKernelDesc KernelXxhScalar;
#endif
#if defined(_arm64_) && !defined(_win_)
extern const TKernelDesc KernelXxhNeon;
#endif
extern const TKernelDesc KernelCity64;

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
