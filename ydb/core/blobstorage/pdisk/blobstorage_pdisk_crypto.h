#pragma once
#include "defs.h"

#include <ydb/core/base/compile_time_flags.h>
#include <ydb/core/blobstorage/crypto/crypto.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// PDiskHashCalculator
////////////////////////////////////////////////////////////////////////////

class TPDiskHashCalculator : public THashCalculator {
    bool UseT1ha0Hasher;

public:
    TPDiskHashCalculator(bool useT1ha0Hasher)
        : UseT1ha0Hasher(useT1ha0Hasher)
    {}

    void SetUseT1ha0Hasher(bool x) {
        UseT1ha0Hasher = x;
    };

    ui64 OldHashSector(const ui64 sectorOffset, const ui64 magic, const ui8 *sector,
            const ui32 sectorSize) {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&sectorOffset, sizeof sectorOffset);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&magic, sizeof magic);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(sector, sectorSize - sizeof(ui64));

        THashCalculator::Clear();
        THashCalculator::Hash(&sectorOffset, sizeof sectorOffset);
        THashCalculator::Hash(&magic, sizeof magic);
        THashCalculator::Hash(sector, sectorSize - sizeof(ui64));
        return THashCalculator::GetHashResult();
    }

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
        if (UseT1ha0Hasher) {
            return T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sector, sectorSize);
        } else {
            return OldHashSector(sectorOffset, magic, sector, sectorSize);
        }
    }

    bool CheckSectorHash(const ui64 sectorOffset, const ui64 magic, const ui8 *sector,
            const ui32 sectorSize, const ui64 sectorHash) {
        // On production servers may be two versions.
        // If by default used OldHash version, then use it first
        // If by default used T1ha0NoAvx version, then use it
        if (UseT1ha0Hasher) {
            return sectorHash == T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sector, sectorSize)
                || sectorHash == OldHashSector(sectorOffset, magic, sector, sectorSize);
        } else {
            return sectorHash == OldHashSector(sectorOffset, magic, sector, sectorSize)
                || sectorHash == T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sector, sectorSize);
        }
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
