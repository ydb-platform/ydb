#define XXH_INLINE_ALL
#define XXH_VECTOR 4 /* XXH_NEON */
#include "xxHash/xxhash.h"

#include "kernels.h"

struct TXxhNeonOp {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        return XXH3_64bits(p, len);
    }
};

static ui64 RunXxhNeon(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TXxhNeonOp>(d, ws, bs, p, ctx);
}

static void* CreateXxhNeonCtx() {
    return new TXxhNeonOp();
}

static void DestroyXxhNeonCtx(void* c) {
    delete static_cast<TXxhNeonOp*>(c);
}

// NEON is baseline on AArch64 (unlike SSE2/AVX2 on x86, there's no runtime
// feature to gate on), so IsSupported is nullptr just like xxh3-sse2.
Y_KERNEL_EXPORT const TKernelDesc KernelXxhNeon{"xxh3-neon", nullptr, CreateXxhNeonCtx, DestroyXxhNeonCtx, RunXxhNeon};

// One-shot export for --functional (ai_func.md §4.1): XXH3_64bits above is
// `static` inside this TU (XXH_INLINE_ALL), so callers outside this file
// need this trivial wrapper to reach the pinned NEON implementation.
ui64 XxhNeonHashOneShot(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}
