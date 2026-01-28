#include "blobstorage_pdisk.h"
#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_writer.h"

namespace NKikimr {
namespace NPDisk {


////////////////////////////////////////////////////////////////////////////
// TBlockDeviceWrite
////////////////////////////////////////////////////////////////////////////

TBufferedWriter::TBlockDeviceWrite::TBlockDeviceWrite(const TReqId& ReqId, TBuffer::TPtr &&buffer, ui64 StartOffset, ui64 DirtyFrom, ui64 DirtyTo, NWilson::TTraceId *TraceId, TActorSystem* ActorSystem)
    : TBlockDeviceAction(ReqId), Deleter(ActorSystem), Buffer(std::unique_ptr<TBuffer, TReleaseWriteAction>(buffer.Release(), Deleter)), StartOffset(StartOffset), DirtyFrom(DirtyFrom), DirtyTo(DirtyTo), TraceId(*TraceId)
{
}

void TBufferedWriter::TBlockDeviceWrite::DoCall(IBlockDevice &BlockDevice) {
    ui8 *source = Buffer->Data() + DirtyFrom - StartOffset;
    ui32 sizeToWrite = (ui32)(DirtyTo - DirtyFrom);
    BlockDevice.PwriteAsync(source, sizeToWrite, DirtyFrom, Buffer.release(), ReqId, &TraceId);
}

TBufferedWriter::TBlockDeviceWrite::TReleaseWriteAction::TReleaseWriteAction(TActorSystem *ActorSystem) : ActorSystem(ActorSystem) {}

void TBufferedWriter::TBlockDeviceWrite::TReleaseWriteAction::operator()(TBuffer *buffer) const {
    if (buffer->FlushAction) {
        //frees buffer->FlushAction
        buffer->FlushAction->Release(ActorSystem);
    }

    buffer->ReturnToPool();
}


////////////////////////////////////////////////////////////////////////////
// TBlockDeviceFlush
////////////////////////////////////////////////////////////////////////////
TBufferedWriter::TBlockDeviceFlush::TBlockDeviceFlush(const TReqId& ReqId, TCompletionAction* completion, TActorSystem* actorSystem)
    : TBlockDeviceAction(ReqId), Deleter(actorSystem), Completion(std::unique_ptr<TCompletionAction, TReleaseFlushAction>(completion, Deleter))
{
}

void TBufferedWriter::TBlockDeviceFlush::DoCall(IBlockDevice &BlockDevice) {
    BlockDevice.FlushAsync(Completion.release(), ReqId);
}

TBufferedWriter::TBlockDeviceFlush::TReleaseFlushAction::TReleaseFlushAction(TActorSystem *ActorSystem) : ActorSystem(ActorSystem) {}

void TBufferedWriter::TBlockDeviceFlush::TReleaseFlushAction::operator()(TCompletionAction *action) const {
    if (action->FlushAction) {
        action->FlushAction->Release(ActorSystem);
    }
    //frees action->FlushAction
    action->Release(ActorSystem);
}

////////////////////////////////////////////////////////////////////////////
// BufferedWriter
////////////////////////////////////////////////////////////////////////////
void TBufferedWriter::WriteToBuffer(TReqId reqId, NWilson::TTraceId *traceId,
        TCompletionAction *flushAction, ui32 chunkIdx) {
    static NWilson::TTraceId noTrace;
    if (DirtyFrom != DirtyTo) {
        ui8 *source = CurrentBuffer->Data() + DirtyFrom - StartOffset;
        ui32 sizeToWrite = (ui32)(DirtyTo - DirtyFrom);
        REQUEST_VALGRIND_CHECK_MEM_IS_DEFINED(source, sizeToWrite);
        CurrentBuffer->FlushAction = flushAction;
        CurrentBuffer->CostNs = DriveModel->TimeForSizeNs(sizeToWrite, chunkIdx, TDriveModel::OP_TYPE_WRITE);
        Y_VERIFY_DEBUG_S(sizeToWrite <= CurrentBuffer->Size(), PCtx->PDiskLogPrefix);
        if (WithDelayedFlush) {
            BlockDeviceActions.push(MakeHolder<TBlockDeviceWrite>(reqId, std::move(CurrentBuffer), StartOffset, DirtyFrom, DirtyTo, traceId, PCtx->ActorSystem));
        } else {
            BlockDevice.PwriteAsync(source, sizeToWrite, DirtyFrom, CurrentBuffer.Release(), reqId, traceId);
        }
        CurrentBuffer = TBuffer::TPtr(Pool->Pop());
        CurrentSector = CurrentBuffer->Data();

        StartOffset = DirtyTo;
        DirtyFrom = DirtyTo;
    } else if (flushAction) {
        flushAction->CostNs = 1;
        if (WithDelayedFlush) {
            BlockDeviceActions.push(MakeHolder<TBlockDeviceFlush>(reqId, flushAction, PCtx->ActorSystem));
        } else {
            BlockDevice.FlushAsync(flushAction, reqId);
        }
    }
}

TBufferedWriter::TBufferedWriter(ui64 sectorSize, IBlockDevice &blockDevice, TDiskFormat &format, TBufferPool *pool,
        TActorSystem *actorSystem, TDriveModel *driveModel, std::shared_ptr<TPDiskCtx> pCtx, bool withDelayedFlush)
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
    , PCtx(std::move(pCtx))
    , WithDelayedFlush(withDelayedFlush)
{
}

void TBufferedWriter::SetupWithBuffer(ui64 startOffset, ui64 currentOffset, TBuffer *buffer, ui32 count, TReqId reqId) {
    Y_VERIFY_S(!WithDelayedFlush, "SetupWithBuffer should not have DelayedFlush");
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
    Y_VERIFY_S(count > 0, PCtx->PDiskLogPrefix);
    Y_VERIFY_S(count <= 16, PCtx->PDiskLogPrefix);
    if (NextOffset != offset || NextOffset + SectorSize * reserve - StartOffset > CurrentBuffer->Size()) {
        WriteToBuffer(LastReqId, traceId, nullptr, chunkIdx);
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
    Y_VERIFY_S(CurrentSector, PCtx->PDiskLogPrefix);
    return CurrentSector;
}

ui8* TBufferedWriter::RawData() const {
    return CurrentBuffer->Data();
}

void TBufferedWriter::Flush(TReqId reqId, NWilson::TTraceId *traceId,
        TCompletionAction *flushAction, ui32 chunkIdx) {
    WriteToBuffer(reqId, traceId, flushAction, chunkIdx);
}

void TBufferedWriter::MarkDirty() {
    DirtyTo = NextOffset;
}

void TBufferedWriter::Obliterate() {
    DirtyTo = DirtyFrom;
}

void TBufferedWriter::WriteToBlockDevice() {
    Y_VERIFY(WithDelayedFlush, "WriteToBlockDevice should be called only for buffers WithDelayedFlush");
    while (!BlockDeviceActions.empty()) {
        const auto& action = BlockDeviceActions.front();
        action->DoCall(BlockDevice);
        BlockDeviceActions.pop();
    }
}

TBufferedWriter::~TBufferedWriter() {
    BlockDeviceActions = {};
    Flush(LastReqId, nullptr, nullptr, 0);
}

} // NPDisk
} // NKikimr
