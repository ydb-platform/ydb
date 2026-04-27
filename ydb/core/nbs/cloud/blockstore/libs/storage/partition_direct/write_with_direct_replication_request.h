#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/write_request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TWriteWithDirectReplicationRequestExecutor
    : public TBaseWriteRequestExecutor
{
public:
    TWriteWithDirectReplicationRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlockRange64 vChunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        ui64 lsn,
        NWilson::TTraceId traceId,
        TDuration hedgingDelay);

    ~TWriteWithDirectReplicationRequestExecutor() override = default;

    void Run() override;

private:
    void SendWriteRequestsToHandoffPBuffers();
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
