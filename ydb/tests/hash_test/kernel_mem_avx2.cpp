#include "kernels.h"

#include <util/system/cpu_id.h>

#include <immintrin.h>

struct TMemReadAvx2Op {
    Y_FORCE_INLINE ui64 ProcessBlock(ui8* p, size_t len) {
        const __m256i* w = reinterpret_cast<const __m256i*>(p);
        size_t n = len / 32;
        __m256i a0 = _mm256_setzero_si256();
        __m256i a1 = _mm256_setzero_si256();
        __m256i a2 = _mm256_setzero_si256();
        __m256i a3 = _mm256_setzero_si256();
        __m256i a4 = _mm256_setzero_si256();
        __m256i a5 = _mm256_setzero_si256();
        __m256i a6 = _mm256_setzero_si256();
        __m256i a7 = _mm256_setzero_si256();
        for (size_t i = 0; i + 8 <= n; i += 8) {
            a0 = _mm256_xor_si256(a0, _mm256_load_si256(w + i + 0));
            a1 = _mm256_xor_si256(a1, _mm256_load_si256(w + i + 1));
            a2 = _mm256_xor_si256(a2, _mm256_load_si256(w + i + 2));
            a3 = _mm256_xor_si256(a3, _mm256_load_si256(w + i + 3));
            a4 = _mm256_xor_si256(a4, _mm256_load_si256(w + i + 4));
            a5 = _mm256_xor_si256(a5, _mm256_load_si256(w + i + 5));
            a6 = _mm256_xor_si256(a6, _mm256_load_si256(w + i + 6));
            a7 = _mm256_xor_si256(a7, _mm256_load_si256(w + i + 7));
        }
        __m256i acc = _mm256_xor_si256(
            _mm256_xor_si256(_mm256_xor_si256(a0, a1), _mm256_xor_si256(a2, a3)),
            _mm256_xor_si256(_mm256_xor_si256(a4, a5), _mm256_xor_si256(a6, a7)));
        __m128i lo = _mm256_castsi256_si128(acc);
        __m128i hi = _mm256_extracti128_si256(acc, 1);
        __m128i x = _mm_xor_si128(lo, hi);
        ui64 r = _mm_extract_epi64(x, 0) ^ _mm_extract_epi64(x, 1);
        return r;
    }
};

static ui64 RunMemReadAvx2(ui8* d, size_t ws, size_t bs, ui64 p, void* ctx) {
    return RunLoop<TMemReadAvx2Op>(d, ws, bs, p, ctx);
}

static bool IsMemReadAvx2Supported() {
    return NX86::CachedHaveAVX2();
}

static void* CreateMemReadAvx2Ctx() {
    return new TMemReadAvx2Op();
}

static void DestroyMemReadAvx2Ctx(void* c) {
    delete static_cast<TMemReadAvx2Op*>(c);
}

Y_KERNEL_EXPORT const TKernelDesc KernelMemReadAvx2{
    "mem-read-avx2",
    IsMemReadAvx2Supported,
    CreateMemReadAvx2Ctx,
    DestroyMemReadAvx2Ctx,
    RunMemReadAvx2,
};
