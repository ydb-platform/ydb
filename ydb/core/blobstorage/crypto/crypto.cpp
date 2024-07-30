#include "crypto.h"
#include "secured_block.h"
#include "util/system/cpu_id.h"

// ifunc is broken at least under msan and tsan, so disable it
// There is no need to use fastest resolved function since specific
// function is used in performance critical code
#define T1HA0_RUNTIME_SELECT 0
#define T1HA_USE_INDIRECT_FUNCTIONS 0
#include <contrib/libs/t1ha/t1ha.h>

namespace NKikimr {
////////////////////////////////////////////////////////////////////////////
// KeyContainer
////////////////////////////////////////////////////////////////////////////

TCypherKey::TCypherKey() {
}

TCypherKey::TCypherKey(const ui8 *key, ui32 keySizeBytes) {
    SetKey(key, keySizeBytes);
}

TCypherKey::TCypherKey(const TCypherKey &a) {
    if (a.IsKeySet) {
        SetKey(a.Key8, sizeof(a.Key8));
    }
}

void TCypherKey::SetKey(const ui8 *key, ui32 keySizeBytes) {
    if (IsKeySet) {
        Wipe();
    }
    if (keySizeBytes) {
        ui32 offset = 0;
        while (offset < sizeof(Key8)) {
            ui32 size = Min((ui32)sizeof(Key8) - offset, keySizeBytes);
            memcpy(Key8 + offset, key, size);
            offset += size;
        }
        IsKeySet = true;
    }
}

bool TCypherKey::GetIsKeySet() const {
    return IsKeySet;
}

ui32 TCypherKey::GetKeySizeBytes() const {
    return 32;
}

bool TCypherKey::GetKeyBytes(const ui8** outKey, ui32 *outSizeBytes) const {
    *outSizeBytes = GetKeySizeBytes();
    *outKey = Key8;
    return IsKeySet;
}

void TCypherKey::MutableKeyBytes(ui8** outKey, ui32 *outSizeBytes) {
    IsKeySet = true;
    *outSizeBytes = GetKeySizeBytes();
    *outKey = Key8;
}

void TCypherKey::Wipe() {
    SecureWipeBuffer(Key8, sizeof(Key8));
    IsKeySet = false;
}

TCypherKey::~TCypherKey() {
    Wipe();
}


////////////////////////////////////////////////////////////////////////////
// HashCalculator
////////////////////////////////////////////////////////////////////////////

THashCalculator::THashCalculator() {
    Poly = std::make_unique<Poly1305Vec>();
    Clear();
}

void THashCalculator::Hash(const void* data, ui64 size) {
    Poly->Update((const ui8*)data, size);
}

ui64 THashCalculator::GetHashResult() {
    Poly->Finish(HashBytes);
    return HashResult[0];
}

ui64 THashCalculator::GetHashResult(ui64 *outHash2) {
    Poly->Finish(HashBytes);
    *outHash2 = HashResult[1];
    return HashResult[0];
}

void THashCalculator::Clear() {
    Poly->SetKey(Key, sizeof(Key));
}

ui32 THashCalculator::GetKeySizeBytes() const {
    return 32;
}

void THashCalculator::SetKey(const ui8 *key, ui32 sizeBytes) {
    Y_ABORT_UNLESS(sizeBytes);
    ui32 offset = 0;
    while (offset < GetKeySizeBytes()) {
        ui32 size = Min(GetKeySizeBytes() - offset, sizeBytes);
        memcpy(Key + offset, key, size);
        offset += size;
    }
}

bool THashCalculator::SetKey(const TCypherKey &key) {
    const ui8* keyBytes = nullptr;
    ui32 keySize = 0;
    bool isKeySet = key.GetKeyBytes(&keyBytes, &keySize);
    SetKey(keyBytes, keySize);
    return isKeySet;
}

THashCalculator::~THashCalculator() {
    SecureWipeBuffer(Key, GetKeySizeBytes());
}

////////////////////////////////////////////////////////////////////////////
// T1haHasher
////////////////////////////////////////////////////////////////////////////

template<ET1haFunc func_type>
void TT1ha0HasherBase<func_type>::SetKey(const ui64 key) {
    T1haSeed = key;
}

template<ET1haFunc func_type>
ui64 TT1ha0HasherBase<func_type>::Hash(const void* data, ui64 size) const {
#if !defined(_arm64_)
    if constexpr (func_type == ET1haFunc::T1HA0_NO_AVX) {
        return t1ha0_ia32aes_noavx(data, size, T1haSeed);
    } else if constexpr (func_type == ET1haFunc::T1HA0_AVX) {
        return t1ha0_ia32aes_avx(data, size, T1haSeed);
    } else if constexpr (func_type == ET1haFunc::T1HA0_AVX2) {
        return t1ha0_ia32aes_avx2(data, size, T1haSeed);
    }
#else
    return t1ha0(data, size, T1haSeed);
#endif
}

template<ET1haFunc func_type>
TT1ha0HasherBase<func_type>::~TT1ha0HasherBase() {
    SecureWipeBuffer((ui8*)&T1haSeed, sizeof T1haSeed);
}

template class TT1ha0HasherBase<ET1haFunc::T1HA0_NO_AVX>;
template class TT1ha0HasherBase<ET1haFunc::T1HA0_AVX>;
template class TT1ha0HasherBase<ET1haFunc::T1HA0_AVX2>;

////////////////////////////////////////////////////////////////////////////
// StreamCypher
////////////////////////////////////////////////////////////////////////////
#define CYPHER_ROUNDS 8

const bool TStreamCypher::HasAVX512 = NX86::HaveAVX512F();

Y_FORCE_INLINE void TStreamCypher::Encipher(const ui8* plaintext, ui8* ciphertext, size_t len) { 
#ifdef __AVX512F__
    std::visit([&](auto&& chacha) {
        chacha.Encipher(plaintext, ciphertext, len);
    }, *Cypher);
#else
    Cypher->Encipher(plaintext, ciphertext, len);
#endif
}

Y_FORCE_INLINE void TStreamCypher::SetKeyAndIV(const ui64 blockIdx) {
#ifdef __AVX512F__
    std::visit([&](auto&& chacha) {
        chacha.SetKey((ui8*)&Key[0], sizeof(Key));
        chacha.SetIV((ui8*)&Nonce, (ui8*)&blockIdx);
    }, *Cypher);
#else
    Cypher->SetKey((ui8*)&Key[0], sizeof(Key));
    Cypher->SetIV((ui8*)&Nonce, (ui8*)&blockIdx);
#endif
}

TStreamCypher::TStreamCypher()
    : Nonce(0)
    , UnusedBytes(0)
{
#if ENABLE_ENCRYPTION
    memset(Key, 0, sizeof(Key));

#ifdef __AVX512F__
    auto* chacha = new std::variant<ChaChaVec, ChaCha512>;
    
    if (HasAVX512) {
        chacha->emplace<ChaCha512>(CYPHER_ROUNDS);
    } else {
        chacha->emplace<ChaChaVec>(CYPHER_ROUNDS);
    }
    Cypher.reset(chacha);
#else
    Cypher.reset(new ChaChaVec(CYPHER_ROUNDS));
#endif
#else
    Y_UNUSED(Leftover);
    Y_UNUSED(Key);
    Y_UNUSED(Nonce);
    Y_UNUSED(Cypher);
    Y_UNUSED(UnusedBytes);
#endif
}

void TStreamCypher::SetKey(const ui64 &key) {
#if ENABLE_ENCRYPTION
    Key[0] = key;
    Key[1] = key;
    Key[2] = key;
    Key[3] = key;
#else
    Y_UNUSED(key);
#endif
}

void TStreamCypher::SetKey(const ui8 *key, ui32 sizeBytes) {
#if ENABLE_ENCRYPTION
    Y_ABORT_UNLESS(sizeBytes);
    ui8 *key8 = (ui8*)&Key;
    ui32 offset = 0;
    while (offset < sizeBytes) {
        ui32 size = Min(GetKeySizeBytes() - offset, sizeBytes);
        memcpy(key8 + offset, key, size);
        offset += size;
    }
#else
    Y_UNUSED(key);
    Y_UNUSED(sizeBytes);
#endif
}

bool TStreamCypher::SetKey(const TCypherKey &key) {
    const ui8* keyBytes = nullptr;
    ui32 keySize = 0;
    bool isKeySet = key.GetKeyBytes(&keyBytes, &keySize);
    SetKey(keyBytes, keySize);
    return isKeySet;
}

ui32 TStreamCypher::GetKeySizeBytes() const {
    return 32;
}

void TStreamCypher::StartMessage(ui64 nonce, ui64 offsetBytes) {
#if ENABLE_ENCRYPTION
    Nonce = nonce;
    UnusedBytes = 0;
    ui64 blockIdx = offsetBytes / ChaChaVec::BLOCK_SIZE;
    SetKeyAndIV(blockIdx);
    ui64 bytesToSkip = offsetBytes - blockIdx * ChaChaVec::BLOCK_SIZE;
    if (bytesToSkip) {
        alignas(16) ui8 padding[BLOCK_BYTES] = {0};
        InplaceEncrypt(padding, bytesToSkip);
    }
#else
    Y_UNUSED(nonce);
    Y_UNUSED(offsetBytes);
#endif
}

void TStreamCypher::EncryptZeroes(void* destination, ui32 size) {
#if ENABLE_ENCRYPTION
    if (UnusedBytes) {
        if (size <= UnusedBytes) {
            memcpy(destination, Leftover + BLOCK_BYTES - UnusedBytes, size);
            UnusedBytes -= size;
            return;
        }
        memcpy(destination, Leftover + BLOCK_BYTES - UnusedBytes, UnusedBytes);
        destination = (ui8*)destination + UnusedBytes;
        size -= UnusedBytes;
        UnusedBytes = 0;
    }
    alignas(16) ui8 zero[BLOCK_BYTES] = {0};
    while (size >= BLOCK_BYTES) {
        Encipher((const ui8*)zero, (ui8*)destination, BLOCK_BYTES);
        destination = (ui8*)destination + BLOCK_BYTES;
        size -= BLOCK_BYTES;
    }
    if (size) {
        Encipher((const ui8*)zero, (ui8*)Leftover, BLOCK_BYTES);
        memcpy(destination, Leftover, size);
        UnusedBytes = BLOCK_BYTES - size;
    }
#else
    memset(destination, 0, size);
#endif
}

#if ENABLE_ENCRYPTION
static void Xor(void* destination, const void* a, const void* b, ui32 size) {
    ui8 *dst = (ui8*)destination;
    const ui8 *srcA = (const ui8*)a;
    const ui8 *srcB = (const ui8*)b;

#ifdef __AVX512F__
    // Process 64 bytes at a time with AVX-512
    size_t i;
    for (i = 0; i + 63 < size; i += 64) {
        __m512i vA = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(srcA + i));
        __m512i vB = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(srcB + i));
        __m512i vXor = _mm512_xor_si512(vA, vB);
        _mm512_storeu_si512(reinterpret_cast<__m512i*>(dst + i), vXor);
    }

    // Process remaining bytes
    for (; i < size; ++i) {
        dst[i] = srcA[i] ^ srcB[i];
    }
#else
    ui8 *endDst = dst + size;
    while (dst != endDst) {
        *dst = *srcA ^ *srcB;
        ++dst;
        ++srcA;
        ++srcB;
    }
#endif
}

static void Xor(TRope::TIterator destination, TRope::TConstIterator a, const void* b, ui32 size) {
    while (size) {
        ui8 *dst = (ui8*)destination.ContiguousData();
        const ui8 *srcA = (const ui8*)a.ContiguousData();
        const ui8 *srcB = (const ui8*)b;
        const ui32 blockSize = Min(destination.ContiguousSize(), a.ContiguousSize());
        ui32 offset = 0;
        for (offset = 0; offset < blockSize && offset < size; offset++) {
            *dst = *srcA ^ *srcB;
            ++dst;
            ++srcA;
            ++srcB;
        }
        destination += offset;
        a += offset;
        b = srcB;
        size -= offset;
    }
}
#endif

void TStreamCypher::Encrypt(void* destination, const void* source, ui32 size) {
#if ENABLE_ENCRYPTION
    if (UnusedBytes) {
        if (size <= UnusedBytes) {
            Xor(destination, source, Leftover + BLOCK_BYTES - UnusedBytes, size);
            UnusedBytes -= size;
            return;
        }
        Xor(destination, source, Leftover + BLOCK_BYTES - UnusedBytes, UnusedBytes);
        size -= UnusedBytes;
        destination = (ui8*)destination + UnusedBytes;
        source = (const ui8*)source + UnusedBytes;
        UnusedBytes = 0;
    }

    ui32 tail = size % BLOCK_BYTES;
    ui32 largePart = size - tail;
    if (largePart) {
        if ((intptr_t)(const ui8*)source % 8 == 0) {
            if ((intptr_t)(ui8*)destination % 16 == 0) {
                Encipher((const ui8*)source, (ui8*)destination, largePart);
                destination = (ui8*)destination + largePart;
                source = (const ui8*)source + largePart;
            } else {
                alignas(16) ui8 data[BLOCK_BYTES] = {0};
                while (size >= BLOCK_BYTES) {
                    Encipher((const ui8*)source, (ui8*)data, BLOCK_BYTES);
                    memcpy(destination, data, BLOCK_BYTES);
                    destination = (ui8*)destination + BLOCK_BYTES;
                    source = (ui8*)source + BLOCK_BYTES;
                    size -= BLOCK_BYTES;
                }
                SecureWipeBuffer(data, BLOCK_BYTES);
            }
        } else {
            if ((intptr_t)(ui8*)destination % 16 == 0) {
                alignas(16) ui8 data[BLOCK_BYTES] = {0};
                while (size >= BLOCK_BYTES) {
                    memcpy(data, source, BLOCK_BYTES);
                    Encipher((const ui8*)data, (ui8*)destination, BLOCK_BYTES);
                    destination = (ui8*)destination + BLOCK_BYTES;
                    source = (ui8*)source + BLOCK_BYTES;
                    size -= BLOCK_BYTES;
                }
                SecureWipeBuffer(data, BLOCK_BYTES);
            } else {
                alignas(16) ui8 data[BLOCK_BYTES] = {0};
                while (size >= BLOCK_BYTES) {
                    memcpy(data, source, BLOCK_BYTES);
                    Encipher((const ui8*)data, (ui8*)data, BLOCK_BYTES);
                    memcpy(destination, data, BLOCK_BYTES);
                    destination = (ui8*)destination + BLOCK_BYTES;
                    source = (ui8*)source + BLOCK_BYTES;
                    size -= BLOCK_BYTES;
                }
                SecureWipeBuffer(data, BLOCK_BYTES);
            }
        }
    }
    if (tail) {
        alignas(16) ui8 zero[BLOCK_BYTES] = {0};
        Encipher((const ui8*)zero, (ui8*)Leftover, BLOCK_BYTES);
        Xor(destination, source, Leftover, tail);
        UnusedBytes = BLOCK_BYTES - tail;
        SecureWipeBuffer(zero, BLOCK_BYTES);
    }
#else
    memcpy(destination, source, size);
#endif
}

void TStreamCypher::Encrypt(TRope::TIterator destination, TRope::TIterator source, ui32 size) {
#if ENABLE_ENCRYPTION
    if (UnusedBytes) {
        if (size <= UnusedBytes) {
            Xor(destination, source, Leftover + BLOCK_BYTES - UnusedBytes, size);
            UnusedBytes -= size;
            return;
        }
        Xor(destination, source, Leftover + BLOCK_BYTES - UnusedBytes, UnusedBytes);
        size -= UnusedBytes;
        destination += UnusedBytes;
        source += UnusedBytes;
        UnusedBytes = 0;
    }

    if (Y_UNLIKELY(size == 0)) { // prevent slideView init
        return;
    }

    constexpr ui32 PACK = 8 * BLOCK_BYTES;
    TRopeSlideView<PACK> slideDst(destination);
    TRopeSlideView<PACK> slideSrc(source);

    while (size >= PACK) {
        Encrypt(slideDst.GetHead(), slideSrc.GetHead(), PACK);
        slideDst.FlushBlock();

        slideDst += PACK;
        slideSrc += PACK;
        size -= PACK;
    }
    Encrypt(slideDst.GetHead(), slideSrc.GetHead(), size);
    slideDst.FlushBlock();
#else
    TRopeUtils::Memcpy(destination, source, size);
#endif
}

void TStreamCypher::InplaceEncrypt(void *source, ui32 size) {
#if ENABLE_ENCRYPTION
    Encrypt(source, source, size);
#else
    Y_UNUSED(source);
    Y_UNUSED(size);
#endif
}

void TStreamCypher::InplaceEncrypt(TRope::TIterator source, ui32 size) {
#if ENABLE_ENCRYPTION
    Encrypt(source, source, size);
#else
    Y_UNUSED(source);
    Y_UNUSED(size);
#endif
}

TStreamCypher::~TStreamCypher() {
    SecureWipeBuffer((ui8*)Key, GetKeySizeBytes());
    SecureWipeBuffer((ui8*)Leftover, sizeof(Leftover));
}

} // NKikimr

