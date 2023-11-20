#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_writer.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////
// BufferedWriter
////////////////////////////////////////////////////////////////////////////

void TBufferedWriter::WriteBufferWithFlush(TReqId reqId, NWilson::TTraceId *traceId,
        TCompletionAction *flushAction, ui32 chunkIdx) {
    static NWilson::TTraceId noTrace;
    if (DirtyFrom != DirtyTo) {
        ui8 *source = CurrentBuffer->Data() + DirtyFrom - StartOffset;
        ui32 sizeToWrite = (ui32)(DirtyTo - DirtyFrom);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(source, sizeToWrite);
        CurrentBuffer->FlushAction = flushAction;
        CurrentBuffer->CostNs = DriveModel->TimeForSizeNs(sizeToWrite, chunkIdx, TDriveModel::OP_TYPE_WRITE);
        Y_DEBUG_ABORT_UNLESS(sizeToWrite <= CurrentBuffer->Size());
        BlockDevice.PwriteAsync(source, sizeToWrite, DirtyFrom, CurrentBuffer.Release(), reqId, traceId);
        CurrentBuffer = TBuffer::TPtr(Pool->Pop());
        CurrentSector = CurrentBuffer->Data();
        StartOffset = DirtyTo;
        DirtyFrom = DirtyTo;
    } else if (flushAction) {
        flushAction->CostNs = 1;
        BlockDevice.FlushAsync(flushAction, reqId);
    }
}

TBufferedWriter::TBufferedWriter(ui64 sectorSize, IBlockDevice &blockDevice, TDiskFormat &format, TBufferPool *pool,
        TActorSystem *actorSystem, TDriveModel *driveModel)
    : SectorSize(sectorSize)
    , BlockDevice(blockDevice)
    , Format(format)
    , StartOffset(0)
    , NextOffset(0)
    , DirtyFrom(0)
    , DirtyTo(0)
    , CurrentSector(nullptr)
    , Pool(pool)
    , CurrentBuffer(Pool->Pop())
    , ActorSystem(actorSystem)
    , LastReqId(TReqId::InitialTSectorWriterReqId, 0)
    , DriveModel(driveModel)
{
}

void TBufferedWriter::SetupWithBuffer(ui64 startOffset, ui64 currentOffset, TBuffer *buffer, ui32 count, TReqId reqId) {
    CurrentBuffer.Reset(buffer);
    CurrentSector = CurrentBuffer->Data() + (currentOffset - startOffset);

    StartOffset = startOffset;
    DirtyFrom = currentOffset;
    DirtyTo = currentOffset;

    NextOffset = currentOffset + SectorSize * count;
    LastReqId = reqId;
    return;
}

ui8* TBufferedWriter::Seek(ui64 offset, ui32 count, ui32 reserve, TReqId reqId, NWilson::TTraceId *traceId,
        ui32 chunkIdx) {
    Y_ABORT_UNLESS(count > 0);
    Y_ABORT_UNLESS(count <= 16);
    if (NextOffset != offset || NextOffset + SectorSize * reserve - StartOffset > CurrentBuffer->Size()) {
        WriteBufferWithFlush(LastReqId, traceId, nullptr, chunkIdx);
        StartOffset = offset;
        DirtyFrom = offset;
        DirtyTo = offset;
    }
    CurrentSector = CurrentBuffer->Data() + (offset - StartOffset);
    NextOffset = offset + SectorSize * count;
    LastReqId = reqId;
    return CurrentSector;
}

ui8* TBufferedWriter::Get() const {
    Y_ABORT_UNLESS(CurrentSector);
    return CurrentSector;
}

ui8* TBufferedWriter::RawData() const {
    return CurrentBuffer->Data();
}

void TBufferedWriter::Flush(TReqId reqId, NWilson::TTraceId *traceId,
        TCompletionAction *flushAction, ui32 chunkIdx) {
    WriteBufferWithFlush(reqId, traceId, flushAction, chunkIdx);
}

void TBufferedWriter::MarkDirty() {
    DirtyTo = NextOffset;
}

void TBufferedWriter::Obliterate() {
    DirtyTo = DirtyFrom;
}

TBufferedWriter::~TBufferedWriter() {
    Flush(LastReqId, nullptr, nullptr, 0);
}

} // NPDisk
} // NKikimr

