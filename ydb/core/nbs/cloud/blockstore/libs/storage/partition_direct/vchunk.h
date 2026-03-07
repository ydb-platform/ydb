#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunk: public std::enable_shared_from_this<TVChunk>
{
public:
    TVChunk(
        ui32 index,
        NStorage::NPartitionDirect::IDirectBlockGroupPtr directBlockGroup,
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
    void RequestBlockFlush(ui64 blockIndex, const NWilson::TTraceId& traceId);
    void ProcessSyncQueue(
        size_t persistBufferIndex,
        const NWilson::TTraceId& traceId);

    ui32 Index;
    size_t BlocksCount;
    ui32 SyncRequestsBatchSize;

    TExecutorPtr Executor;
    std::unique_ptr<TDirtyMap> DirtyMap;
    NStorage::NPartitionDirect::IDirectBlockGroupPtr DirectBlockGroup;

    static constexpr size_t PersistentBufferCount = 5;
    TVector<TVector<TSyncRequest>> PendingSyncRequestsByPersistentBufferIndex;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
