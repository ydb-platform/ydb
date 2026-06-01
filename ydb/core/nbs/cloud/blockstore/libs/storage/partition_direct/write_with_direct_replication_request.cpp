#include "write_with_direct_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteWithDirectReplicationRequestExecutor::
    TWriteWithDirectReplicationRequestExecutor(
        NActors::TActorSystem* actorSystem,
        TChildLogTitle logTitle,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TBlockRange64 vChunkRange,
        TCallContextPtr callContext,
        std::shared_ptr<TWriteBlocksLocalRequest> request,
        ui64 lsn,
        NWilson::TTraceId traceId)
    : TBaseWriteRequestExecutor(
          actorSystem,
          std::move(logTitle),
          vChunkConfig,
          std::move(directBlockGroup),
          vChunkRange,
          std::move(callContext),
          std::move(request),
          lsn,
          std::move(traceId))
{}

void TWriteWithDirectReplicationRequestExecutor::Run()
{
    ScheduleRequestTimeoutCallback();
    ScheduleHedging();
    for (auto host: VChunkConfig.GetDesiredPBuffers()) {
        SendWriteRequest(host);
    }
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
                if (!self->IsAlreadyReplied()) {
                    self->SendWriteRequestsToHandoffPBuffers();
                }
            }
        });
}

void TWriteWithDirectReplicationRequestExecutor::
    SendWriteRequestsToHandoffPBuffers()
{
    const auto availableHandOffHosts = GetAvailableHandOffHosts();
    const size_t neededHedgingRequestsCount = std::min(
        QuorumDirectBlockGroupHostCount - CompletedWrites.Count(),
        availableHandOffHosts.size());

    for (size_t i = 0; i < neededHedgingRequestsCount; ++i) {
        const auto host = availableHandOffHosts[i];
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteWithDirectReplicationRequestExecutor. Send write request to "
            "handoff host %u since we "
            "have %lu completed writes",
            PrintHostIndex(host).c_str(),
            CompletedWrites.Count());

        SendWriteRequest(host);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
