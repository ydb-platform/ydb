#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

// @brief
// This class allows to use persistent buffer's replication mechanism.
// The object sends single request to one of PB and this PB replicates
//   request to another 2 PB.
// In case of error object sends required number of direct write requests
//   with possible retries.
// Also it schedules hedge requests that work in the same way as
//   retries mechanism.
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
        NWilson::TTraceId traceId);

    ~TWriteWithPbReplicationRequestExecutor() override = default;

    void Run() override;

private:
    // Hosts which can be used for direct write requests for retry or hedge.
    // it includes hosts that were not a direct destination
    // of any write request - f.e. secondary hosts from ManyPBuffers request.
    // It excludes hosts for which responses have been received.
    THostMask AvailableHostsForDirectSending;
    size_t ActiveDirectWritesNumber{};
    const TDuration PbufferReplyTimeout;

    void SendWriteRequestToManyPBuffers(TVector<THostIndex> hosts);
    void OnWriteToManyPBuffersResponse(
        const TDBGWriteBlocksToManyPBuffersResponse& response);
    void TryToSendDirectWrites(bool isHedge);
    void OnWriteResponse(
        THostIndex hosts,
        const TDBGWriteBlocksResponse& response,
        std::shared_ptr<NWilson::TSpan> span) override;

    void ScheduleHedging() override;
    void SendDirectWriteRequest(THostIndex host);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
