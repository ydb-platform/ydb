#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TWriteWithPbReplicationRequestExecutor: public TBaseWriteRequestExecutor
{
public:
    TWriteWithPbReplicationRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlockRange64 vChunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        ui64 lsn,
        NWilson::TTraceId traceId,
        TDuration hedgingDelay,
        TDuration timeout,
        TDuration pbufferReplyTimeout);

    ~TWriteWithPbReplicationRequestExecutor() override = default;

    void Run() override;

private:
    // набор локаций, в которые не было прямых походов + из которых не было
    // явных ответов
    TSet<ELocation> AvailableLocationsForDirectSending;

    ui32 NumberOfDirectWritesInProgress{};
    const TDuration PbufferReplyTimeout;

    void SendWriteRequestToManyPBuffers(TVector<ELocation> locations);
    void OnWriteToManyPBuffersResponse(
        const TDBGWriteBlocksToManyPBuffersResponse& response);
    void TryToSendDirectWrites(bool isHedge = false);
    void OnWriteResponse(
        ELocation location,
        const TDBGWriteBlocksResponse& response,
        std::shared_ptr<NWilson::TSpan> span) override;

    void ScheduleHedging() override;
    void SendDirectWriteRequest(ELocation location);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
