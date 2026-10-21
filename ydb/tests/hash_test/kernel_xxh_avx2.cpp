#define XXH_INLINE_ALL
#define XXH_VECTOR 2 /* XXH_AVX2 */
#include "xxHash/xxhash.h"

#include "kernels.h"

#include <util/system/cpu_id.h>

struct TXxhAvx2Op {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        return XXH3_64bits(p, len);
    }
};

static ui64 RunXxhAvx2(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TXxhAvx2Op>(d, ws, bs, p, ctx);
}

static bool IsXxhAvx2Supported() {
    return NX86::CachedHaveAVX2();
}

static void* CreateXxhAvx2Ctx() {
    return new TXxhAvx2Op();
}

static void DestroyXxhAvx2Ctx(void* c) {
    delete static_cast<TXxhAvx2Op*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelXxhAvx2{
    "xxh3-avx2",
    IsXxhAvx2Supported,
    CreateXxhAvx2Ctx,
    DestroyXxhAvx2Ctx,
    RunXxhAvx2,
};

// One-shot export for --functional (ai_func.md §4.1): XXH3_64bits above is
// `static` inside this TU (XXH_INLINE_ALL), so callers outside this file
// need this trivial wrapper to reach the pinned AVX2 implementation. Keep
// this TU minimal otherwise -- everything in it compiles with -mavx2 and
// must never run before the runtime IsXxhAvx2Supported() gate passes.
ui64 XxhAvx2HashOneShot(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}
