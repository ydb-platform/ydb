#include "variants.h"

#include <ydb/core/blobstorage/crypto/chacha.h>
#if !(defined(_win_) || defined(_arm64_))
#include <ydb/core/blobstorage/crypto/chacha_vec.h>
#include <ydb/core/blobstorage/crypto/chacha_512/chacha_512.h>
#include "xxHash/xxh_x86dispatch.h"
#endif

#include <util/generic/array_size.h>
#include <util/system/cpu_id.h>

namespace {

#if !(defined(_win_) || defined(_arm64_))
bool IsAvx2Supported() {
    return NX86::CachedHaveAVX2();
}

bool IsAvx512Supported() {
    return NX86::CachedHaveAVX512F();
}

ui64 XxhDispatchHashOneShot(const void* data, size_t len) {
    return XXH3_64bits_dispatch(data, len);
}

// ChaChaVec::Encipher hard-aborts (Y_ABORT) unless the plaintext pointer is
// 0 or 8 mod 16 (chacha_vec.cpp); the equivalence sweep deliberately probes
// other misalignments and must skip this variant there.
bool IsChaChaVecAlignmentOk(const void* p) {
    const ui32 a = static_cast<ui32>(reinterpret_cast<intptr_t>(p) % 16);
    return a == 0 || a == 8;
}
#endif

void ChaChaIntEncipher(
    const ui8 key[32], const ui8 iv[8], ui64 counter64,
    const ui8* src, ui8* dst, size_t len)
{
    ChaCha cipher;
    cipher.SetKey(key, ChaCha::KEY_SIZE);
    cipher.SetIV(iv, reinterpret_cast<const ui8*>(&counter64));
    cipher.Encipher(src, dst, len);
}

#if !(defined(_win_) || defined(_arm64_))
void ChaChaSseEncipher(
    const ui8 key[32], const ui8 iv[8], ui64 counter64,
    const ui8* src, ui8* dst, size_t len)
{
    ChaChaVec cipher;
    cipher.SetKey(key, ChaChaVec::KEY_SIZE);
    cipher.SetIV(iv, reinterpret_cast<const ui8*>(&counter64));
    cipher.Encipher(src, dst, len);
}

void ChaChaAvx512Encipher(
    const ui8 key[32], const ui8 iv[8], ui64 counter64,
    const ui8* src, ui8* dst, size_t len)
{
    // Caller must have already checked IsAvx512Supported(): constructing
    // ChaCha512 executes AVX-512 instructions in SetKey/SetIV (ai_func.md
    // §4.3 note; same pitfall as ai.md §6.3).
    ChaCha512 cipher;
    cipher.SetKey(key, ChaCha512::KEY_SIZE);
    cipher.SetIV(iv, reinterpret_cast<const ui8*>(&counter64));
    cipher.Encipher(src, dst, len);
}
#endif

const THashVariant Xxh3Variants[] = {
#if !defined(_win_)
    {"xxh3-scalar", nullptr, XxhScalarHashOneShot},
#endif
#if !(defined(_win_) || defined(_arm64_))
    {"xxh3-sse2", nullptr, XxhSse2HashOneShot},
    {"xxh3-avx2", IsAvx2Supported, XxhAvx2HashOneShot},
    {"xxh3-dispatch", nullptr, XxhDispatchHashOneShot},
#endif
#if defined(_arm64_) && !defined(_win_)
    {"xxh3-neon", nullptr, XxhNeonHashOneShot},
#endif
};

const TCipherVariant ChaChaVariants[] = {
    {"chacha-int", nullptr, nullptr, ChaChaIntEncipher},
#if !(defined(_win_) || defined(_arm64_))
    {"chacha-sse", nullptr, IsChaChaVecAlignmentOk, ChaChaSseEncipher},
    {"chacha-avx512", IsAvx512Supported, nullptr, ChaChaAvx512Encipher},
#endif
};

} // namespace

const THashVariant* GetXxh3Variants(size_t* count) {
    *count = Y_ARRAY_SIZE(Xxh3Variants);
    return Xxh3Variants;
}

const TCipherVariant* GetChaChaVariants(size_t* count) {
    *count = Y_ARRAY_SIZE(ChaChaVariants);
    return ChaChaVariants;
}
