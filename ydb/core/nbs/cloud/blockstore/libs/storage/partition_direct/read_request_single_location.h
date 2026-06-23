#pragma once

#include "read_request_executor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/public.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/log_title.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/dirty_map.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/vchunk_config.h>

#include <ydb/library/actors/core/actorsystem.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// Class works with a single readHint. It performs a request into a 1 source
// location
// ATTENTION: you should use fabric method CreateReadRequestExecutor
class TReadSingleLocationRequestExecutor
    : public IReadRequestExecutor
    , public std::enable_shared_from_this<TReadSingleLocationRequestExecutor>
{
public:
    TReadSingleLocationRequestExecutor(
        NActors::TActorSystem const* actorSystem,
        const TLogTitle& logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadRangeHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TReadSingleLocationRequestExecutor() override;

    // Implementation of IRequestExecutor
    void Run() override;
    TString Print() override;

    // Implementation of IReadRequestExecutor
    [[nodiscard]] NThreading::TFuture<TResponse> GetFuture() const override;

private:
    void StartReading();
    void OnReadResponse(
        THostIndex host,
        const TDBGReadBlocksResponse& response);
    void Reply(NProto::TError error);

    void ScheduleHedging();
    void ScheduleRequestTimeout();
    void OnHedgingTimeout();
    void OnRequestTimeout();

    NActors::TActorSystem const* ActorSystem;
    const TChildLogTitle LogTitle;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;
    const TDuration HedgingDelay;
    const TDuration RequestTimeout;

    TReadRangeHint ReadHint;
    THostMask Requested;
    THostMask Failed;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
};

using TReadSingleLocationRequestExecutorPtr =
    std::shared_ptr<TReadSingleLocationRequestExecutor>;

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
