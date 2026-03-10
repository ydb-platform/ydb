#include "vchunk.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    ui32 index,
    NStorage::NPartitionDirect::IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize)
    : Index(index)
    , BlocksCount(128 * 1024 * 1024 / 4096)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , DirectBlockGroup(std::move(directBlockGroup))
    , PendingSyncRequestsByPersistentBufferIndex(PersistentBufferCount)
{
    Executor = TExecutor::Create(TStringBuilder() << "VChunk_" << Index);
    Executor->Start();

    DirtyMap = std::make_unique<TDirtyMap>();
}

void TVChunk::Start()
{
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this()]() mutable
        {
            if (auto self = weakSelf.lock()) {
                auto future = self->DirectBlockGroup->EstablishConnections(
                    self->Executor,
                    {},   // traceId
                    self->Index);

                self->Executor->WaitFor(future);

                const auto restoreMeta =
                    self->DirectBlockGroup->RestoreFromPersistentBuffers(
                        self->Executor,
                        {},   // traceId
                        self->Index);
                for (const auto& meta: restoreMeta) {
                    self->DirtyMap->TryUpdateLsnByPersistentBufferIndex(
                        meta.BlockIndex,
                        meta.PersistBufferIndex,
                        meta.Lsn);
                }
            }
        });
}

TVChunk::~TVChunk()
{
    if (Executor) {
        Executor->Stop();
    }
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse> TVChunk::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto promise = NThreading::NewPromise<TReadBlocksLocalResponse>();
    auto future = promise.GetFuture();

    if (request->Range.Start >= BlocksCount) {
        promise.SetValue(TReadBlocksLocalResponse{
            .Error = MakeError(E_FAIL, "out of range")});

        return future;
    }

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            if (auto self = weakSelf.lock()) {
                const ui64 blockIndex = request->Range.Start;

                if (!self->DirtyMap->IsBlockWritten(blockIndex)) {
                    auto guard = request->Sglist.Acquire();
                    if (guard) {
                        const auto& sglist = guard.Get();
                        const auto& block = sglist[0];
                        memset(
                            const_cast<char*>(block.Data()),
                            0,
                            block.Size());
                    }
                    TReadBlocksLocalResponse response;
                    response.Error = MakeError(S_OK);
                    promise.SetValue(std::move(response));
                    return;
                }

                const bool readFromPersistentBuffer =
                    !self->DirtyMap->IsBlockFlushedToDDisk(blockIndex);

                if (readFromPersistentBuffer) {
                    ui8 persistentBufferIndex = 0;
                    ui64 lsn = self->DirtyMap->GetLsnByPersistentBufferIndex(
                        blockIndex,
                        persistentBufferIndex);

                    self->DirectBlockGroup->ReadBlocksLocalFromPersistentBuffer(
                        self->Executor,
                        self->Index,
                        persistentBufferIndex,
                        std::move(callContext),
                        std::move(request),
                        std::move(traceId),
                        std::move(promise),
                        lsn);
                } else {
                    self->DirectBlockGroup->ReadBlocksLocalFromDDisk(
                        self->Executor,
                        self->Index,
                        std::move(callContext),
                        std::move(request),
                        std::move(traceId),
                        std::move(promise));
                }
            }
        });

    return future;
}

NThreading::TFuture<TWriteBlocksLocalResponse> TVChunk::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    auto promise = NThreading::NewPromise<TWriteBlocksLocalResponse>();
    auto future = promise.GetFuture();

    if (request->Range.Start >= BlocksCount) {
        promise.SetValue(TWriteBlocksLocalResponse{
            .Error = MakeError(E_FAIL, "out of range")});

        return future;
    }

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            if (auto self = weakSelf.lock()) {
                auto startIndex = request->Range.Start;
                const auto& writtenBlocksMeta =
                    self->DirectBlockGroup->WriteBlocksLocal(
                        self->Executor,
                        self->Index,
                        std::move(callContext),
                        std::move(request),
                        traceId.GetTraceId(),
                        std::move(promise));

                for (const auto& meta: writtenBlocksMeta) {
                    self->DirtyMap->OnBlockWriteCompleted(
                        startIndex,
                        TPersistentBufferWriteMeta(meta.Index, meta.Lsn));
                }

                self->RequestBlockFlush(startIndex, traceId.GetTraceId());
            }
        });

    return future;
}

////////////////////////////////////////////////////////////////////////////////

void TVChunk::RequestBlockFlush(
    ui64 blockIndex,
    const NWilson::TTraceId& traceId)
{
    for (size_t i = 0; i < PersistentBufferCount; i++) {
        PendingSyncRequestsByPersistentBufferIndex[i].emplace_back(
            blockIndex,
            DirtyMap->GetLsnByPersistentBufferIndex(blockIndex, i));

        if (PendingSyncRequestsByPersistentBufferIndex[i].size() >=
            SyncRequestsBatchSize)
        {
            ProcessSyncQueue(i, traceId);
        }
    }
}

void TVChunk::ProcessSyncQueue(
    size_t persistBufferIndex,
    const NWilson::TTraceId& traceId)
{
    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         persistBufferIndex,
         traceId = NWilson::TTraceId(traceId)]() mutable
        {
            if (auto self = weakSelf.lock()) {
                auto syncRequests =
                    std::move(self->PendingSyncRequestsByPersistentBufferIndex
                                  [persistBufferIndex]);
                self->PendingSyncRequestsByPersistentBufferIndex
                    [persistBufferIndex] = {};

                self->DirectBlockGroup->SyncWithPersistentBuffer(
                    self->Executor,
                    self->Index,
                    persistBufferIndex,
                    syncRequests,
                    std::move(traceId));

                for (const auto& syncRequest: syncRequests) {
                    self->DirtyMap->OnBlockFlushCompleted(
                        syncRequest.StartIndex,
                        persistBufferIndex,
                        syncRequest.Lsn);
                }
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
