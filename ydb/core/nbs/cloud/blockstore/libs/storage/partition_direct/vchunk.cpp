#include "vchunk.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t PersistentBufferCount = 5;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVChunk::TVChunk(
    ui32 index,
    IDirectBlockGroupPtr directBlockGroup,
    ui32 syncRequestsBatchSize)
    : Index(index)
    , BlocksCount(128 * 1024 * 1024 / 4096)
    , SyncRequestsBatchSize(syncRequestsBatchSize)
    , Executor(TExecutor::Create(TStringBuilder() << "VChunk_" << Index))
    , DirtyMap(std::make_unique<TDirtyMap>())
    , DirectBlockGroup(std::move(directBlockGroup))
    , PendingSyncRequestsByPersistentBufferIndex(PersistentBufferCount)
{
    Executor->Start();
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
    Executor->Stop();
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse> TVChunk::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // VHost thread

    if (request->Range.Start >= BlocksCount) {
        return MakeFuture<TReadBlocksLocalResponse>(TReadBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "out of range")});
    }

    auto promise = NThreading::NewPromise<TReadBlocksLocalResponse>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            // Executor thread

            if (auto self = weakSelf.lock()) {
                self->DoReadBlocksLocal(
                    std::move(promise),
                    std::move(callContext),
                    std::move(request),
                    std::move(traceId));
            } else {
                promise.SetValue(
                    TReadBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

NThreading::TFuture<TWriteBlocksLocalResponse> TVChunk::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // VHost thread

    if (request->Range.Start >= BlocksCount) {
        return MakeFuture<TWriteBlocksLocalResponse>(TWriteBlocksLocalResponse{
            .Error = MakeError(E_FAIL, "out of range")});
    }

    auto promise = NThreading::NewPromise<TWriteBlocksLocalResponse>();
    auto future = promise.GetFuture();

    Executor->ExecuteSimple(
        [weakSelf = weak_from_this(),
         promise = std::move(promise),
         callContext = std::move(callContext),
         request = std::move(request),
         traceId = std::move(traceId)]() mutable
        {
            // Executor thread

            if (auto self = weakSelf.lock()) {
                self->DoWriteBlocksLocal(
                    std::move(promise),
                    std::move(callContext),
                    std::move(request),
                    std::move(traceId));
            } else {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
            }
        });

    return future;
}

////////////////////////////////////////////////////////////////////////////////

void TVChunk::DoReadBlocksLocal(
    TPromise<TReadBlocksLocalResponse> promise,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // Executor thread
    const ui64 blockIndex = request->Range.Start;

    if (!DirtyMap->IsBlockWritten(blockIndex)) {
        if (auto guard = request->Sglist.Acquire()) {
            const auto& sglist = guard.Get();
            const auto& block = sglist[0];
            memset(const_cast<char*>(block.Data()), 0, block.Size());
        }
        TReadBlocksLocalResponse response;
        response.Error = MakeError(S_OK);
        // Note! Do not set promise when sgList acquired.
        promise.SetValue(std::move(response));
        return;
    }

    const bool readFromPersistentBuffer =
        !DirtyMap->IsBlockFlushedToDDisk(blockIndex);

    TFuture<TDBGReadBlocksResponse> future;

    if (readFromPersistentBuffer) {
        ui8 persistentBufferIndex = 0;
        ui64 lsn = DirtyMap->GetLsnByPersistentBufferIndex(
            blockIndex,
            persistentBufferIndex);
        future = DirectBlockGroup->ReadBlocksLocalFromPersistentBuffer(
            Index,
            persistentBufferIndex,
            std::move(callContext),
            std::move(request),
            std::move(traceId),
            lsn);
    } else {
        future = DirectBlockGroup->ReadBlocksLocalFromDDisk(
            Index,
            std::move(callContext),
            std::move(request),
            std::move(traceId));
    }

    future.Subscribe(
        [executor = Executor, promise = std::move(promise)]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f) mutable
        {
            // ActorSystem thread
            executor->ExecuteSimple(
                [promise = std::move(promise),
                 value = UnsafeExtractValue(f)]   //
                () mutable
                {
                    // Executor thread
                    promise.SetValue(TReadBlocksLocalResponse{
                        .Error = std::move(value.Error)});
                });
        });
}

void TVChunk::DoWriteBlocksLocal(
    TPromise<TWriteBlocksLocalResponse> promise,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    // Executor thread

    auto range = request->Range;
    auto future = DirectBlockGroup->WriteBlocksLocal(
        Index,
        std::move(callContext),
        std::move(request),
        traceId.GetTraceId());
    future.Subscribe(
        [weakSelf = weak_from_this(),
         range,
         traceId = traceId.GetTraceId(),
         promise = std::move(promise)]   //
        (const NThreading::TFuture<TDBGWriteBlocksResponse>& f) mutable
        {
            // ActorSystem thread
            auto self = weakSelf.lock();
            if (!self) {
                promise.SetValue(
                    TWriteBlocksLocalResponse{.Error = MakeError(E_CANCELLED)});
                return;
            }
            self->Executor->ExecuteSimple(
                [weakSelf = std::move(weakSelf),
                 range,
                 traceId,
                 promise = std::move(promise),
                 value = UnsafeExtractValue(f)]   //
                () mutable
                {
                    // Executor thread
                    if (auto self = weakSelf.lock()) {
                        self->OnWriteBlocksResponse(
                            std::move(promise),
                            range,
                            traceId,
                            std::move(value));
                    }
                });
        });
}

void TVChunk::OnWriteBlocksResponse(
    NThreading::TPromise<TWriteBlocksLocalResponse> promise,
    TBlockRange64 range,
    ui64 traceId,
    TDBGWriteBlocksResponse response)
{
    // Executor thread

    promise.SetValue(TWriteBlocksLocalResponse{.Error = response.Error});

    for (const auto& meta: response.Meta) {
        DirtyMap->OnBlockWriteCompleted(
            range.Start,
            TPersistentBufferWriteMeta(meta.Index, meta.Lsn));
    }

    RequestBlockFlush(range.Start, traceId);
}

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
