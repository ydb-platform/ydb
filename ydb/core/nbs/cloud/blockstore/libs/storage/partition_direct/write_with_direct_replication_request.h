#pragma once

#include "write_request.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TWriteWithDirectReplicationRequestExecutor
    : public TBaseWriteRequestExecutor
{
public:
    TWriteWithDirectReplicationRequestExecutor(
        NActors::TActorSystem* actorSystem,
        TChildLogTitle logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        std::shared_ptr<TWriteRequestBundle> bundle);

    ~TWriteWithDirectReplicationRequestExecutor() override = default;

    void Run() override;

private:
    void SendWriteRequestsToHandoffPBuffers();

    void ScheduleHedging() override;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
