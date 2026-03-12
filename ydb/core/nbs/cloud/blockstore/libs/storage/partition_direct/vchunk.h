#pragma once

#include "direct_block_group.h"
#include "dirty_map.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunk: public std::enable_shared_from_this<TVChunk>
{
public:
    TVChunk(
        ui32 index,
        IDirectBlockGroupPtr directBlockGroup,
        ui32 syncRequestsBatchSize);

    ~TVChunk();

    void Start();

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

private:
    void DoReadBlocksLocal(
        NThreading::TPromise<TReadBlocksLocalResponse> promise,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    void DoWriteBlocksLocal(
        NThreading::TPromise<TWriteBlocksLocalResponse> promise,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        NWilson::TTraceId traceId);
    void OnWriteBlocksResponse(
        NThreading::TPromise<TWriteBlocksLocalResponse> promise,
        TBlockRange64 range,
        ui64 traceId,
        TDBGWriteBlocksResponse response);

    void RequestBlockFlush(ui64 blockIndex, const NWilson::TTraceId& traceId);
    void ProcessSyncQueue(
        size_t persistBufferIndex,
        const NWilson::TTraceId& traceId);
    void OnBlocksFlushed(
        size_t persistBufferIndex,
        const TVector<TSyncRequest>& syncRequests,
        const TDBGSyncBlocksResponse& response);

    const ui32 Index;
    const size_t BlocksCount;
    const ui32 SyncRequestsBatchSize;

    TExecutorPtr Executor;
    std::unique_ptr<TDirtyMap> DirtyMap;
    IDirectBlockGroupPtr DirectBlockGroup;
    TVector<TVector<TSyncRequest>> PendingSyncRequestsByPersistentBufferIndex;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
