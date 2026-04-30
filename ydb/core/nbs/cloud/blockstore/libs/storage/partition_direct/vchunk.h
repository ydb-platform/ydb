#pragma once

#include "public.h"

#include "erase_request.h"
#include "flush_request.h"
#include "vchunk_config.h"
#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/thread_checker.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/vchunk_counters.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TVChunk: public std::enable_shared_from_this<TVChunk>
{
public:
    TVChunk(
        NActors::TActorSystem* actorSystem,
        IPartitionDirectService* partitionDirectService,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        ui32 syncRequestsBatchSize,
        ui64 vChunkSize,
        TDuration writeHedgingDelay,
        TDuration writeRequestTimeout,
        TDuration traceSamplePeriod,
        NMonitoring::TDynamicCounterPtr counters);

    ~TVChunk();

    void Start();

    NThreading::TFuture<TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        const NWilson::TTraceId& traceId);

    NThreading::TFuture<TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        EWriteMode writeMode,
        TDuration pbufferReplyTimeout,
        ui64 lsn,
        const NWilson::TTraceId& traceId);

private:
    void UpdateDirtyMap(const TDBGRestoreResponse& response);

    void DoStart();

    void DoReadBlocksLocal(
        TTracedPromise<TReadBlocksLocalResponse> promise,
        TBlockRange64 vchunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        std::shared_ptr<NWilson::TSpan> span);

    void DoWriteBlocksLocal(
        TTracedPromise<TWriteBlocksLocalResponse> promise,
        TBlockRange64 vchunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        EWriteMode writeMode,
        TDuration pbufferReplyTimeout,
        ui64 lsn,
        std::shared_ptr<NWilson::TSpan> span);
    void OnWriteBlocksResponse(
        TTracedPromise<TWriteBlocksLocalResponse> promise,
        TBlockRange64 range,
        const TBaseWriteRequestExecutor::TResponse& response,
        std::shared_ptr<NWilson::TSpan> span);

    void DoFlush();
    void OnFlushResponse(const TFlushRequestExecutor::TResponse& response);

    void DoErase();
    void OnEraseResponse(const TEraseRequestExecutor::TResponse& response);

    NActors::TActorSystem* const ActorSystem = nullptr;
    IPartitionDirectService* const PartitionDirectService = nullptr;
    const TExecutorPtr Executor;
    const TThreadChecker ExecutorThreadChecker{Executor};
    const IDirectBlockGroupPtr DirectBlockGroup;
    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;
    const TVChunkConfig VChunkConfig;
    const THostMask DDiskReadable;
    const THostMask DDiskFlushTargets;
    const THostMask PBufferActive;
    const ui32 BlockSize;
    const ui64 BlocksCount;
    const ui32 SyncRequestsBatchSize;
    const TDuration WriteHedgingDelay;
    const TDuration WriteRequestTimeout;
    const TDuration TraceSamplePeriod;

    TDDiskStateList DDiskStates;
    TBlocksDirtyMap BlocksDirtyMap;
    bool DirtyMapRestored = false;

    TVChunkCounters Counters;

    void UpdatePendingCounters();
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
