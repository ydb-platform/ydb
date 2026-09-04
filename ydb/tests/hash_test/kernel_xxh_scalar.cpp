#define XXH_INLINE_ALL
#define XXH_VECTOR 0 /* XXH_SCALAR */
#include "xxHash/xxhash.h"

#include "kernels.h"

struct TXxhScalarOp {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        return XXH3_64bits(p, len);
    }
};

static ui64 RunXxhScalar(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TXxhScalarOp>(d, ws, bs, p, ctx);
}

static void* CreateXxhScalarCtx() {
    return new TXxhScalarOp();
}

static void DestroyXxhScalarCtx(void* c) {
    delete static_cast<TXxhScalarOp*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelXxhScalar{"xxh3-scalar", nullptr, CreateXxhScalarCtx, DestroyXxhScalarCtx, RunXxhScalar};

// One-shot export for --functional (ai_func.md §4.1): XXH3_64bits above is
// `static` inside this TU (XXH_INLINE_ALL), so callers outside this file
// need this trivial wrapper to reach the pinned scalar implementation.
ui64 XxhScalarHashOneShot(const void* data, size_t len) {
    return XXH3_64bits(data, len);
}
