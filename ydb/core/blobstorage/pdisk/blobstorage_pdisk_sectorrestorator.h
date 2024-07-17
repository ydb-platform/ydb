#pragma once

#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_data.h"
#include "blobstorage_pdisk_mon.h"
#include "defs.h"

namespace NKikimr {
namespace NPDisk {

struct TSectorRestorator {
    const bool IsTrippleCopy;
    const ui32 ErasureDataParts;
    ui32 LastGoodIdx;
    ui32 LastBadIdx;
    ui32 GoodSectorFlags;
    ui32 GoodSectorCount;
    ui32 RestoredSectorFlags;
    const TDiskFormat &Format;
    TActorSystem *ActorSystem;
    const TActorId PDiskActorId;
    ui32 PDiskId;
    bool IsErasureEncode;
    TPDiskMon *Mon;
    TBufferPool *BufferPool;

    TSectorRestorator(const bool isTrippleCopy, const ui32 erasureDataParts,
            const bool isErasureEncode, const TDiskFormat &format,
            TActorSystem *actorSystem, const TActorId pDiskActorId, ui32 pDiskId, TPDiskMon *mon,
            TBufferPool *bufferPool);

    TSectorRestorator(const bool isTrippleCopy, const ui32 erasureDataParts,
            const bool isErasureEncode, const TDiskFormat &format);


    void Restore(ui8 *source, const ui64 offset, const ui64 magic, const ui64 lastNonce, TOwner owner);

    void WriteSector(ui8 *sectorData, ui64 writeOffset);
};

} // NPDisk
} // NKikimr
