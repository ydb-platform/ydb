#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/crypto/crypto.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// PDiskHashCalculator
////////////////////////////////////////////////////////////////////////////

class TPDiskHashCalculator : public THashCalculator {
public:
    template<class THasher>
    ui64 T1ha0HashSector(const ui64 sectorOffset, const ui64 magic, const ui8 *sector,
            const ui32 sectorSize) {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&sectorOffset, sizeof sectorOffset);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&magic, sizeof magic);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(sector, sectorSize - sizeof(ui64));

        THasher hasher;
        hasher.SetKey(sectorOffset ^ magic);
        return hasher.Hash(sector, sectorSize - sizeof(ui64));
    }

    ui64 HashSector(const ui64 sectorOffset, const ui64 magic, const ui8 *sector,
            const ui32 sectorSize) {
        return T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sector, sectorSize);
    }

    bool CheckSectorHash(const ui64 sectorOffset, const ui64 magic, const ui8 *sector,
            const ui32 sectorSize, const ui64 sectorHash) {
        return sectorHash == T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sector, sectorSize);
    }
};

////////////////////////////////////////////////////////////////////////////
// PDiskStreamCypher
////////////////////////////////////////////////////////////////////////////

class TPDiskStreamCypher {
    TStreamCypher Impl;
    const bool EnableEncryption = true;
public:

    TPDiskStreamCypher(bool encryption)
        : Impl()
        , EnableEncryption(encryption)
    {}

    void SetKey(const ui64 &key) {
        if (EnableEncryption) {
            Impl.SetKey(key);
        }
    }

    void StartMessage(ui64 nonce) {
        if (EnableEncryption) {
            Impl.StartMessage(nonce, 0);
        }
    }

    void EncryptZeroes(void* destination, ui32 size) {
        if (EnableEncryption) {
            Impl.EncryptZeroes(destination, size);
        } else {
            memset(destination, 0, size);
        }
    }

    void Encrypt(void* destination, const void* source, ui32 size) {
        if (EnableEncryption) {
            Impl.Encrypt(destination, source, size);
        } else {
            memcpy(destination, source, size);
        }
    }

    void InplaceEncrypt(void *source, ui32 size) {
        if (EnableEncryption) {
            Impl.InplaceEncrypt(source, size);
        }
    }
};

} // NPDisk
} // NKikimr
