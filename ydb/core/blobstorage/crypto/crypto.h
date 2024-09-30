#pragma once
#include <ydb/library/actors/util/rope.h>
#include <util/generic/ptr.h>
#include <util/system/cpu_id.h>
#include <util/system/types.h>

#if (defined(_win_) || defined(_arm64_))
#include <ydb/core/blobstorage/crypto/chacha.h>
#include <ydb/core/blobstorage/crypto/poly1305.h>
#define ChaChaVec ChaCha
#define Poly1305Vec Poly1305
#define CHACHA_BPI 1
#elif __AVX512F__
#include <ydb/core/blobstorage/crypto/chacha_vec.h>
#include <ydb/core/blobstorage/crypto/chacha_512.h>
#include <ydb/core/blobstorage/crypto/poly1305_vec.h>
#else
#include <ydb/core/blobstorage/crypto/chacha_vec.h>
#include <ydb/core/blobstorage/crypto/poly1305_vec.h>
#endif

// Use ENABLE_ENCRYPTION switch for testing purposes only!
#define ENABLE_ENCRYPTION 1

namespace NKikimr {

constexpr ui32 BLOCK_BYTES = ChaChaVec::BLOCK_SIZE * CHACHA_BPI;

////////////////////////////////////////////////////////////////////////////
// KeyContainer
////////////////////////////////////////////////////////////////////////////
class TCypherKey {
    alignas(16) ui8 Key8[32] = {0};
    bool IsKeySet = false;
public:
    TCypherKey();
    TCypherKey(const ui8 *key, ui32 keySizeBytes);
    TCypherKey(const TCypherKey &a);
    void SetKey(const ui8 *key, ui32 keySizeBytes);
    bool GetIsKeySet() const;
    ui32 GetKeySizeBytes() const;
    bool GetKeyBytes(const ui8** outKey, ui32 *outSizeBytes) const;
    void MutableKeyBytes(ui8** outKey, ui32 *outSizeBytes);
    void Wipe();
    ~TCypherKey();

    friend bool operator ==(const TCypherKey& x, const TCypherKey& y) {
        return x.IsKeySet == y.IsKeySet && !memcmp(x.Key8, y.Key8, sizeof(x.Key8));
    }
};

////////////////////////////////////////////////////////////////////////////
// HashCalculator
////////////////////////////////////////////////////////////////////////////

class THashCalculator {
    union {
        ui64 HashResult[2];
        ui8 HashBytes[16];
    };
    std::unique_ptr<Poly1305Vec> Poly;
    alignas(16) ui8 Key[32] = {'p', 'o', 'l', 'y', '1', '3', '0', '5', 'V', 'e', 'c', 'K', 'e', 'y', '1',
        'D', 'u', 'm', 'm', 'y', 'C', 'o', 'n', 's', 't', 'a', 'n', 't', 'V', 'a'};
public:
    THashCalculator();
    void SetKey(const ui8 *key, ui32 sizeBytes);
    bool SetKey(const TCypherKey &key);
    void Hash(const void* data, ui64 size);
    ui64 GetHashResult();
    ui64 GetHashResult(ui64 *outHash2);
    void Clear();
    ui32 GetKeySizeBytes() const;
    ~THashCalculator();
};

enum class ET1haFunc {
    T1HA0_NO_AVX,
    T1HA0_AVX,
    T1HA0_AVX2,
};

template<ET1haFunc func_type>
class TT1ha0HasherBase {
    ui64 T1haSeed = 0;

public:
    void SetKey(const ui64 key);

    ui64 Hash(const void* data, ui64 size) const;

    ~TT1ha0HasherBase();
};

using TT1ha0NoAvxHasher = TT1ha0HasherBase<ET1haFunc::T1HA0_NO_AVX>;
using TT1ha0AvxHasher = TT1ha0HasherBase<ET1haFunc::T1HA0_AVX>;
using TT1ha0Avx2Hasher = TT1ha0HasherBase<ET1haFunc::T1HA0_AVX2>;

////////////////////////////////////////////////////////////////////////////
// StreamCypher
////////////////////////////////////////////////////////////////////////////

class TStreamCypher {
    alignas(16) ui8 Leftover[BLOCK_BYTES];
    alignas(16) ui64 Key[4];
    alignas(16) i64 Nonce;
#ifdef __AVX512F__
    std::unique_ptr<std::variant<ChaChaVec, ChaCha512>> Cypher;
#else
    std::unique_ptr<ChaChaVec> Cypher;
#endif
    ui32 UnusedBytes;
    static const bool HasAVX512;
public:
    TStreamCypher();
    void SetKey(const ui64 &key);
    void SetKey(const ui8 *key, ui32 sizeBytes);
    bool SetKey(const TCypherKey &key);
    ui32 GetKeySizeBytes() const;
    void StartMessage(ui64 nonce, ui64 offsetBytes);
    void EncryptZeroes(void* destination, ui32 size);
    void Encrypt(void* destination, const void* source, ui32 size);
    void Encrypt(TRope::TIterator destination, TRope::TIterator source, ui32 size);
    void InplaceEncrypt(void *source, ui32 size);
    void InplaceEncrypt(TRope::TIterator source, ui32 size);
    ~TStreamCypher();
private:
    void Encipher(const ui8* plaintext, ui8* ciphertext, size_t len);
    void SetKeyAndIV(const ui64 blockIdx);
};

} // NKikimr

