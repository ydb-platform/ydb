#define XXH_INLINE_ALL
#define XXH_VECTOR 1 /* XXH_SSE2 */
#include "xxHash/xxhash.h"

#include "kernels.h"

struct TXxhSse2Op {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        return XXH3_64bits(p, len);
    }
};

static ui64 RunXxhSse2(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TXxhSse2Op>(d, ws, bs, p, ctx);
}

static void* CreateXxhSse2Ctx() {
    return new TXxhSse2Op();
}

static void DestroyXxhSse2Ctx(void* c) {
    delete static_cast<TXxhSse2Op*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelXxhSse2{"xxh3-sse2", nullptr, CreateXxhSse2Ctx, DestroyXxhSse2Ctx, RunXxhSse2};

// One-shot export for --functional (ai_func.md §4.1): XXH3_64bits above is
// `static` inside this TU (XXH_INLINE_ALL), so callers outside this file
// need this trivial wrapper to reach the pinned SSE2 implementation.
ui64 XxhSse2HashOneShot(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}
