#include "kernels.h"

#include <ydb/core/blobstorage/crypto/chacha.h>

struct TChaChaOp {
    ChaCha Cipher;

    TChaChaOp() {
        alignas(16) static const ui8 Key[32] = {
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
            0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
            0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
        };
        static const ui8 Iv[8] = {};
        Cipher.SetKey(Key, 32);
        Cipher.SetIV(Iv);
    }

    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        Cipher.Encipher(p, p, len);
        return 0;
    }
};

static ui64 RunChaChaInt(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TChaChaOp>(d, ws, bs, p, ctx);
}

static void* CreateChaChaCtx() {
    return new TChaChaOp();
}

static void DestroyChaChaCtx(void* c) {
    delete static_cast<TChaChaOp*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelChaChaInt{"chacha-int", nullptr, CreateChaChaCtx, DestroyChaChaCtx, RunChaChaInt};
