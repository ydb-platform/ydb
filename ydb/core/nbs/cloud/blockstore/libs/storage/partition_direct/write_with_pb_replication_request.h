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
        TDuration pbufferReplyTimeout);

    ~TWriteWithPbReplicationRequestExecutor() override = default;

    void Run() override;

private:
    const TDuration PbufferReplyTimeout;

    void SendWriteRequestToManyPBuffers();
    void OnWriteToManyPBuffersResponse(
        const TDBGWriteBlocksToManyPBuffersResponse& response);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
