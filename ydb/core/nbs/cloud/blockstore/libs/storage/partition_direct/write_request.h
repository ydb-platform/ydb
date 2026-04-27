#pragma once

#include "public.h"

#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/direct_block_group.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/host_status.h>

#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TBaseWriteRequestExecutor
    : public std::enable_shared_from_this<TBaseWriteRequestExecutor>
{
public:
    struct TResponse
    {
        NProto::TError Error;
        ui64 Lsn = 0;
        // The PBuffer hosts where the attempt was made to write the data.
        THostMask RequestedWrites;
        // The PBuffer hosts where exactly the data was written and confirmed.
        THostMask CompletedWrites;
    };

    TBaseWriteRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlockRange64 vChunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        ui64 lsn,
        NWilson::TTraceId traceId,
        TDuration hedgingDelay,
        TDuration timeout);

    virtual ~TBaseWriteRequestExecutor();

    [[nodiscard]] NThreading::TFuture<TResponse> GetFuture() const;

    virtual void Run() = 0;

protected:
    void Reply(NProto::TError error);

    void SendWriteRequest(THostIndex host);

    void OnWriteResponse(
        THostIndex host,
        const TDBGWriteBlocksResponse& response,
        std::shared_ptr<NWilson::TSpan> span);

    void ScheduleRequestTimeoutCallback();
    void RequestTimeoutCallback();

    TVector<THostIndex> GetAvailableHandOffHosts() const;

    virtual void ScheduleHedging() = 0;

    NActors::TActorSystem* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TBlockRange64 VChunkRange;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TWriteBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;
    const ui64 Lsn;
    const TDuration HedgingDelay;
    const TDuration RequestTimeout;

    NThreading::TPromise<TResponse> Promise =
        NThreading::NewPromise<TResponse>();
    THostMask RequestedWrites;
    THostMask CompletedWrites;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
