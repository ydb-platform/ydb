#include "kernels.h"

struct TMemReadOp {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        const ui64* w = reinterpret_cast<const ui64*>(p);
        size_t n = len / 8;
        ui64 a0 = 0, a1 = 0, a2 = 0, a3 = 0, a4 = 0, a5 = 0, a6 = 0, a7 = 0;
        for (size_t i = 0; i + 8 <= n; i += 8) {
            a0 ^= w[i + 0];
            a1 ^= w[i + 1];
            a2 ^= w[i + 2];
            a3 ^= w[i + 3];
            a4 ^= w[i + 4];
            a5 ^= w[i + 5];
            a6 ^= w[i + 6];
            a7 ^= w[i + 7];
        }
        return a0 ^ a1 ^ a2 ^ a3 ^ a4 ^ a5 ^ a6 ^ a7;
    }
};

static ui64 RunMemRead(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TMemReadOp>(d, ws, bs, p, ctx);
}

static void* CreateMemCtx() {
    return new TMemReadOp();
}

static void DestroyMemCtx(void* c) {
    delete static_cast<TMemReadOp*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelMemRead{"mem-read", nullptr, CreateMemCtx, DestroyMemCtx, RunMemRead};
