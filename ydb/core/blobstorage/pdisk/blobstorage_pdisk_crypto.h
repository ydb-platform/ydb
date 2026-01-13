#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/crypto/crypto.h>

#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// PDiskHashCalculator
////////////////////////////////////////////////////////////////////////////

class TPDiskHashCalculator : public THashCalculator {
    bool UseT1ha0Hasher;

public:
    // T1ha0 hash is default for current version, but old hash is used to test backward compatibility
    TPDiskHashCalculator(bool useT1ha0Hasher = true)
        : UseT1ha0Hasher(useT1ha0Hasher)
    {}

    ui64 OldHashSector(ui64 sectorOffset, ui64 magic, const ui8 *sector, ui32 sectorSize) {
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
    ui64 T1ha0HashSector(ui64 sectorOffset, ui64 magic, const ui8 *sector, ui32 sectorSize) {
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&sectorOffset, sizeof sectorOffset);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(&magic, sizeof magic);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(sector, sectorSize - sizeof(ui64));

        THasher hasher;
        hasher.SetKey(sectorOffset ^ magic);
        return hasher.Hash(sector, sectorSize - sizeof(ui64));
    }

    ui64 HashSector(ui64 sectorOffset, ui64 magic, const ui8 *sector, ui32 sectorSize, TLogoBlobID blobId) {
        if (UseT1ha0Hasher) {
            return T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic ^ (blobId ? blobId.Hash() : 0), sector, sectorSize);
        } else {
            return OldHashSector(sectorOffset, magic, sector, sectorSize);
        }
    }

    bool CheckSectorHash(ui64 sectorOffset, ui64 magic, const ui8 *sector, ui32 sectorSize, ui64 sectorHash,
            TLogoBlobID blobId) {
        // On production servers may be two versions.
        // If by default used OldHash version, then use it first
        // If by default used T1ha0NoAvx version, then use it
        return (blobId && sectorHash == T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic ^ blobId.Hash(), sector, sectorSize))
            || sectorHash == T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sector, sectorSize)
            || sectorHash == OldHashSector(sectorOffset, magic, sector, sectorSize);
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
#ifdef DISABLE_PDISK_ENCRYPTION
        , EnableEncryption(false && encryption)
#else
        , EnableEncryption(encryption)
#endif
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
