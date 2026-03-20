#pragma once

#include "direct_block_group.h"
#include "erase_request.h"
#include "flush_request.h"
#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/vchunk_config.h>

#include <ydb/core/nbs/cloud/storage/core/libs/coroutine/executor.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunk: public std::enable_shared_from_this<TVChunk>
{
public:
    TVChunk(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        ui32 syncRequestsBatchSize,
        TDuration traceSamplePeriod);

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
    NWilson::TTraceId SpanTrace();

    void UpdateDirtyMap(const TDBGRestoreResponse& response);

    void DoStart();

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
        const TWriteRequestExecutor::TResponse& response);

    void DoFlush();
    void OnFlushResponse(const TFlushRequestExecutor::TResponse& response);

    void DoErase();
    void OnEraseResponse(const TEraseRequestExecutor::TResponse& response);

    NActors::TActorSystem* const ActorSystem = nullptr;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TVChunkConfig VChunkConfig;
    const size_t BlocksCount;
    const ui32 SyncRequestsBatchSize;
    const TDuration TraceSamplePeriod;

    TBlocksDirtyMap BlocksDirtyMap;
    std::atomic<NActors::TMonotonic> LastTraceTs{NActors::TMonotonic::Zero()};
    bool DirtyMapRestored = false;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
