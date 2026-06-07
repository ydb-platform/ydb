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
        std::shared_ptr<TWriteRequestBundle> bundle)
    : TBaseWriteRequestExecutor(
          actorSystem,
          std::move(logTitle),
          vChunkConfig,
          std::move(directBlockGroup),
          std::move(bundle))
{}

void TWriteWithDirectReplicationRequestExecutor::Run()
{
    Bundle->GetSpan().Event("Run");
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
            "%s Send write request to handoff %s since we have %lu completed "
            "writes",
            LogTitle.GetWithTime().c_str(),
            PrintHostIndex(host).c_str(),
            CompletedWrites.Count());

        SendWriteRequest(host);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
