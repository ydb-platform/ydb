#include "kernels.h"

#include <util/system/cpu_id.h>

#include <ydb/core/blobstorage/crypto/chacha_512/chacha_512.h>

struct TChaCha512Op {
    ChaCha512 Cipher;

    TChaCha512Op() {
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

static ui64 RunChaChaAvx512(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TChaCha512Op>(d, ws, bs, p, ctx);
}

static bool IsChaChaAvx512Supported() {
    return NX86::CachedHaveAVX512F();
}

static void* CreateChaCha512Ctx() {
    return new TChaCha512Op();
}

static void DestroyChaCha512Ctx(void* c) {
    delete static_cast<TChaCha512Op*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelChaChaAvx512{
    "chacha-avx512",
    IsChaChaAvx512Supported,
    CreateChaCha512Ctx,
    DestroyChaCha512Ctx,
    RunChaChaAvx512,
};
