#include "blobstorage_pdisk_crypto.h"
#include "blobstorage_pdisk_internal_interface.h"
#include "blobstorage_pdisk_sectorrestorator.h"

namespace NKikimr {
namespace NPDisk {

TSectorRestorator::TSectorRestorator(const bool isTrippleCopy, const ui32 erasureDataParts,
        const bool isErasureEncode, const TDiskFormat &format,
        TActorSystem *actorSystem, const TActorId pDiskActorId, ui32 pDiskId, TPDiskMon *mon,
        TBufferPool *bufferPool)
    : IsTrippleCopy(isTrippleCopy)
    , ErasureDataParts(erasureDataParts)
    , LastGoodIdx((ui32)-1)
    , LastBadIdx((ui32)-1)
    , GoodSectorFlags(0)
    , GoodSectorCount(0)
    , RestoredSectorFlags(0)
    , Format(format)
    , ActorSystem(actorSystem)
    , PDiskActorId(pDiskActorId)
    , PDiskId(pDiskId)
    , IsErasureEncode(isErasureEncode)
    , Mon(mon)
    , BufferPool(bufferPool)
{}

TSectorRestorator::TSectorRestorator(const bool isTrippleCopy, const ui32 erasureDataParts,
        const bool isErasureEncode, const TDiskFormat &format)
    : TSectorRestorator(isTrippleCopy, erasureDataParts, isErasureEncode, format, nullptr, {}, 0, nullptr,
            nullptr)
{}

void TSectorRestorator::Restore(ui8 *source, const ui64 offset, const ui64 magic, const ui64 lastNonce,
        TOwner owner) {
    ui32 sectorCount = IsErasureEncode ? (IsTrippleCopy ? ReplicationFactor : (ErasureDataParts + 1)) : 1;
    ui64 maxNonce = 0;
    TPDiskHashCalculator hasher;
    for (ui32 i = 0; i < sectorCount; ++i) {
        TDataSectorFooter *sectorFooter = (TDataSectorFooter*)
            (source + (i + 1) * Format.SectorSize - sizeof(TDataSectorFooter));
        TParitySectorFooter *paritySectorFooter = (TParitySectorFooter*)
            (source + (i + 1) * Format.SectorSize - sizeof(TParitySectorFooter));

        ui64 sectorOffset = offset + (IsTrippleCopy ? 0 : (ui64)i * (ui64)Format.SectorSize);
        ui8 *sectorData = source + i * Format.SectorSize;
        bool isCrcOk = hasher.CheckSectorHash(sectorOffset, magic, sectorData, Format.SectorSize, sectorFooter->Hash);
        if (!isCrcOk) {
            if (ActorSystem) {
                LOG_INFO_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " <<  (ui32)PDiskId << " Bad hash."
                    << " owner# " << owner
                    << " IsErasureEncode# " << (ui32)IsErasureEncode
                    << " ErasureDataParts# " << (ui32)ErasureDataParts << " i# " << (ui32)i
                    << " readHash# " << (ui64)sectorFooter->Hash
                    << " calculatedT1ha0NoAvxHash# "
                        << hasher.T1ha0HashSector<TT1ha0NoAvxHasher>(sectorOffset, magic, sectorData, Format.SectorSize)
                    << " sectorOffset# " << sectorOffset
                    << " chunkIdx# " << (sectorOffset / (ui64)Format.ChunkSize)
                    << " sectorIdx# " << ((sectorOffset % (ui64)Format.ChunkSize) / (ui64)Format.SectorSize));
            }
            LastBadIdx = i;
        } else if (IsTrippleCopy) {
            ui64 nonce = sectorFooter->Nonce;
            // One with the greatest Nonce is the correct sector
            if (nonce > maxNonce) {
                maxNonce = nonce;
                LastGoodIdx = i;
                GoodSectorFlags = (1u << i);
                GoodSectorCount = 1;
            } else if (nonce == maxNonce) {
                LastGoodIdx = i;
                GoodSectorFlags |= (1 << i);
                ++GoodSectorCount;
            }
        } else {
            ui64 sectorFooterNonce = i < ErasureDataParts ? sectorFooter->Nonce : paritySectorFooter->Nonce;
            if (sectorFooterNonce <= lastNonce || sectorFooterNonce <= maxNonce) {
                if (ActorSystem) {
                    LOG_WARN_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                            << " Sector nonce reordering."
                            << " owner# " << owner
                            << " IsErasureEncode# " << (ui32)IsErasureEncode
                            << " ErasureDataParts# " << (ui32)ErasureDataParts
                            << " i# "  << (ui32)i << " readNonce# " << (ui64)sectorFooterNonce
                            << " lastNonce# " << (ui64)lastNonce << " maxNonce# " << (ui64)maxNonce
                            << " sectorOffset# " << sectorOffset);
                }
                // Consider decreasing nonces to be a sign of write reordering, restore sectors
                LastBadIdx = i;
            } else {
                maxNonce = sectorFooterNonce;
                LastGoodIdx = i;
                GoodSectorFlags |= (1 << i);
                ++GoodSectorCount;
            }

        }
    }

    if (IsErasureEncode) {
        if (!IsTrippleCopy && GoodSectorCount == ErasureDataParts) {
            if (ActorSystem) {
                LOG_WARN_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                        << " owner# " << owner
                        << " Restoring sector. ErasureDataParts# " << (ui32)ErasureDataParts
                        << " LastBadIdx# " << (ui32)LastBadIdx
                        << " sectorOffset# " << (ui64)(offset + (ui64)LastBadIdx * (ui64)Format.SectorSize));
            }
            for (ui32 i = 0; i < Format.SectorSize / sizeof(ui64) - 1; ++i) {
                ui64 restored = 0;
                for (ui32 a = 0; a < LastBadIdx; ++a) {
                    restored ^= ((ui64*)(source + a * Format.SectorSize))[i];
                }
                for (ui32 a = LastBadIdx + 1; a <= ErasureDataParts ; ++a) {
                    restored ^= ((ui64*)(source + a * Format.SectorSize))[i];
                }
                ((ui64*)(source + LastBadIdx * Format.SectorSize))[i] = restored;
            }
            ui8 *sectorData = source + LastBadIdx * Format.SectorSize;
            ui64 sectorOffset = offset + (ui64(LastBadIdx) * ui64(Format.SectorSize));
            if (LastBadIdx == ErasureDataParts) {
                // restoring parity sector
                TParitySectorFooter *sectorFooter = (TParitySectorFooter*)
                    (sectorData + Format.SectorSize - sizeof(TParitySectorFooter));
                TDataSectorFooter *goodDataFooter = (TDataSectorFooter*)
                    (source + (ErasureDataParts) * Format.SectorSize - sizeof(TDataSectorFooter));
                sectorFooter->Nonce = goodDataFooter->Nonce + 1;
                sectorFooter->Hash = hasher.HashSector(sectorOffset, magic, sectorData, Format.SectorSize);
            } else {
                // restoring data sector
                TDataSectorFooter *sectorFooter = (TDataSectorFooter*)
                    (sectorData + Format.SectorSize - sizeof(TDataSectorFooter));
                // TODO: restore the correct Version value
                sectorFooter->Version = PDISK_DATA_VERSION;
                sectorFooter->Hash = hasher.HashSector(sectorOffset, magic, sectorData, Format.SectorSize);
                // Increment here because we don't want to count initialy not written parts
                *Mon->DeviceErasureSectorRestorations += 1;
            }
            GoodSectorFlags |= (1 << LastBadIdx);
            ++GoodSectorCount;
            RestoredSectorFlags |= (1 << LastBadIdx);
            WriteSector(sectorData, sectorOffset);
        } else if (IsTrippleCopy && GoodSectorCount > 0 && GoodSectorCount < ReplicationFactor) {
            ui32 lastGoodSector = 0;
            for (i32 i = ReplicationFactor - 1; i >= 0; --i) {
                if (GoodSectorFlags & (1 << i)) {
                    lastGoodSector = i;
                    break;
                }
            }
            ui32 mask = (1 << lastGoodSector) - 1;
            if ((GoodSectorFlags & mask) != mask) {
                *Mon->DeviceErasureSectorRestorations += 1;
            }
            for (ui32 i = 0; i < ReplicationFactor; ++i) {
                if (!(GoodSectorFlags & (1 << i))) {
                    ui8 *badSector = source + i * Format.SectorSize;
                    ui64 sectorOffset = offset + (ui64)(i * Format.SectorSize);
                    ui8 *goodSector = source + LastGoodIdx * Format.SectorSize;

                    if (ActorSystem) {
                        LOG_WARN_S(*ActorSystem, NKikimrServices::BS_PDISK, "PDiskId# " << (ui32)PDiskId
                                << " Restoring trippleCopy sector i# " << (ui32)i
                                << " owner# " << owner
                                << " GoodSectorCount# " << (ui32)GoodSectorCount
                                << " ReplicationFactor# " << (ui32)ReplicationFactor
                                << " sectorOffset# " << (ui64)sectorOffset);
                    }
                    // Y_ABORT("RESTORE");
                    memcpy(badSector, goodSector, size_t(Format.SectorSize));
                    GoodSectorFlags |= (1 << i);
                    ++GoodSectorCount;
                    RestoredSectorFlags |= (1 << i);
                    WriteSector(badSector, sectorOffset);
                }
            }
        }
    }
}

void TSectorRestorator::WriteSector(ui8 *sectorData, ui64 writeOffset) {
    if (ActorSystem && BufferPool) {
        TBuffer *buffer = BufferPool->Pop();
        Y_ABORT_UNLESS(Format.SectorSize <= buffer->Size());
        memcpy(buffer->Data(), sectorData, (size_t)Format.SectorSize);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(buffer->Data(), Format.SectorSize);
        ActorSystem->Send(PDiskActorId, new TEvLogSectorRestore(buffer->Data(), Format.SectorSize, writeOffset, buffer));
    }
}

} // NPDisk
} // NKikimr
