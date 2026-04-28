#pragma once

#include "public.h"

#include "vchunk_config.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/dirty_map/location.h>

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
        // The PBuffers mask where the attempt was made to write the data.
        TLocationMask RequestedWrites;
        // The PBuffers mask where exactly the data was written and confirmed.
        TLocationMask CompletedWrites;
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

    void SendWriteRequest(ELocation location);

    void OnWriteResponse(
        ELocation location,
        const TDBGWriteBlocksResponse& response,
        std::shared_ptr<NWilson::TSpan> span);

    void ScheduleRequestTimeoutCallback();
    void RequestTimeoutCallback();

    TVector<ELocation> GetAvailableHandOffLocations() const;

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
    TLocationMask RequestedWrites;
    TLocationMask CompletedWrites;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
