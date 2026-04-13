#include "write_with_direct_replication_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteWithDirectReplicationRequestExecutor::
    TWriteWithDirectReplicationRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlockRange64 vChunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        ui64 lsn,
        NWilson::TTraceId traceId,
        TDuration hedgingDelay,
        TDuration timeout)
    : TBaseWriteRequestExecutor(
          actorSystem,
          vChunkConfig,
          std::move(directBlockGroup),
          std::move(vChunkRange),
          std::move(callContext),
          std::move(request),
          lsn,
          std::move(traceId),
          hedgingDelay,
          timeout)
{}

void TWriteWithDirectReplicationRequestExecutor::Run()
{
    ScheduleRequestTimeoutCallback();
    ScheduleHedging();
    SendWriteRequest(ELocation::PBuffer0);
    SendWriteRequest(ELocation::PBuffer1);
    SendWriteRequest(ELocation::PBuffer2);
}

void TWriteWithDirectReplicationRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    DirectBlockGroup->Schedule(
        HedgingDelay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = std::static_pointer_cast<
                    TWriteWithDirectReplicationRequestExecutor>(
                    weakSelf.lock()))
            {
                self->SendWriteRequestsToHandoffPBuffers();
            }
        });
}

void TWriteWithDirectReplicationRequestExecutor::
    SendWriteRequestsToHandoffPBuffers()
{
    if (Promise.IsReady()) {
        return;
    }

    if (CompletedWrites.Count() <= QuorumDirectBlockGroupHostCount - 1 &&
        !RequestedWrites.Get(ELocation::HOPBuffer0))
    {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteWithDirectReplicationRequestExecutor. Send write request to "
            "HOPBuffer0 since we "
            "have %lu completed writes",
            CompletedWrites.Count());

        SendWriteRequest(ELocation::HOPBuffer0);
    }

    if (CompletedWrites.Count() <= QuorumDirectBlockGroupHostCount - 2 &&
        !RequestedWrites.Get(ELocation::HOPBuffer1))
    {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteWithDirectReplicationRequestExecutor. Send write request to "
            "HOPBuffer1 since we "
            "have %lu completed writes",
            CompletedWrites.Count());

        SendWriteRequest(ELocation::HOPBuffer1);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
