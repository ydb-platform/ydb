#include "write_with_direct_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

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

    const auto availableHandOffLocations = GetAvailableHandOffLocations();
    const size_t neededHedgingRequestsCount = std::min(
        QuorumDirectBlockGroupHostCount - CompletedWrites.Count(),
        availableHandOffLocations.size());

    for (size_t i = 0; i < neededHedgingRequestsCount; ++i) {
        const auto& location = availableHandOffLocations[i];
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteWithDirectReplicationRequestExecutor. Send write request to "
            "handoff buffer %lu since we "
            "have %lu completed writes",
            GetLocationIndex(location),
            CompletedWrites.Count());

        SendWriteRequest(location);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
