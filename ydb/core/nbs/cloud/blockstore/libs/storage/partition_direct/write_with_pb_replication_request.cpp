#include "write_with_pb_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteWithPbReplicationRequestExecutor::TWriteWithPbReplicationRequestExecutor(
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
    TDuration pbufferReplyTimeout)
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
    , PbufferReplyTimeout(pbufferReplyTimeout)
{}

void TWriteWithPbReplicationRequestExecutor::Run()
{
    ScheduleRequestTimeoutCallback();
    ScheduleHedging();

    TVector<THostIndex> primaries;
    for (auto h: VChunkConfig.PBufferHosts.GetPrimary()) {
        primaries.push_back(h);
    }
    SendWriteRequestToManyPBuffers(std::move(primaries));
}

void TWriteWithPbReplicationRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    DirectBlockGroup->Schedule(
        HedgingDelay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = std::static_pointer_cast<
                    TWriteWithPbReplicationRequestExecutor>(weakSelf.lock()))
            {
                if (!self->CompletedWrites.Count()) {
                    TVector<THostIndex> secondWave;
                    std::optional<THostIndex> lastPrimary;
                    for (auto h: self->VChunkConfig.PBufferHosts.GetPrimary()) {
                        lastPrimary = h;
                    }
                    if (lastPrimary) {
                        secondWave.push_back(*lastPrimary);
                    }
                    for (auto h: self->VChunkConfig.PBufferHosts.GetHandOff()) {
                        secondWave.push_back(h);
                    }
                    self->SendWriteRequestToManyPBuffers(std::move(secondWave));
                }
            }
        });
}

void TWriteWithPbReplicationRequestExecutor::SendWriteRequestToManyPBuffers(
    TVector<THostIndex> hosts)
{
    if (Promise.IsReady()) {
        return;
    }

    for (auto h: hosts) {
        RequestedWrites.Set(h);
    }

    auto future = DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.VChunkIndex,
        std::move(hosts),
        Lsn,
        VChunkRange,
        PbufferReplyTimeout,
        Request->Sglist,
        NWilson::TTraceId(TraceId));

    future.Subscribe(
        [self =
             std::static_pointer_cast<TWriteWithPbReplicationRequestExecutor>(
                 shared_from_this())](
            const NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>& f)
        { self->OnWriteToManyPBuffersResponse(f.GetValue()); });
}

void TWriteWithPbReplicationRequestExecutor::OnWriteToManyPBuffersResponse(
    const TDBGWriteBlocksToManyPBuffersResponse& response)
{
    if (HasError(response.OverallError)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse fatal error: %s",
            FormatError(response.OverallError).c_str());
        // The error will be set and replied below.
    } else {
        for (const auto& pbufferResponse: response.Responses) {
            const auto h = pbufferResponse.HostIndex;
            if (h >= VChunkConfig.PBufferHosts.HostCount() ||
                VChunkConfig.PBufferHosts.Get(h) == EHostStatus::Disabled)
            {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "OnWriteToManyPBuffersResponse: response for unknown or "
                    "disabled host %u",
                    static_cast<unsigned>(h));
                continue;
            }
            if (!HasError(pbufferResponse.Error)) {
                CompletedWrites.Set(h);
            } else {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "OnWriteToManyPBuffersResponse error on host %u: %s",
                    static_cast<unsigned>(h),
                    FormatError(pbufferResponse.Error).c_str());
            }
        }
    }

    if (CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount) {
        Reply(MakeError(S_OK));
        return;
    }

    const auto availableHandOffHosts = GetAvailableHandOffHosts();
    if (CompletedWrites.Count() + availableHandOffHosts.size() <
        QuorumDirectBlockGroupHostCount)
    {
        auto resultError =
            MakeError(E_FAIL, "Hand-offs retries are not available");
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse: %s",
            FormatError(resultError).c_str());

        Reply(resultError);
        return;
    }

    // Sending request to handoff in case of 1-2 errors
    for (size_t i = 0;
         i < QuorumDirectBlockGroupHostCount - CompletedWrites.Count();
         ++i)
    {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "trying to send fallback writeRequest to %zu handoff",
            i);

        SendWriteRequest(availableHandOffHosts[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
