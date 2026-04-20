#include "write_with_pb_replication_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>

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
    SendWriteRequestToManyPBuffers(
        {ELocation::PBuffer0, ELocation::PBuffer1, ELocation::PBuffer2});
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
                    self->SendWriteRequestToManyPBuffers(
                        {ELocation::PBuffer2,
                         ELocation::HOPBuffer0,
                         ELocation::HOPBuffer1});
                }
            }
        });
}

void TWriteWithPbReplicationRequestExecutor::SendWriteRequestToManyPBuffers(
    TVector<ELocation> locations)
{
    if (Promise.IsReady()) {
        return;
    }

    TVector<ui8> hostsIndexes;
    hostsIndexes.reserve(3);
    for (auto location: locations) {
        hostsIndexes.push_back(VChunkConfig.GetHostIndex(location));
        RequestedWrites.Set(location);
    }

    auto future = DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.VChunkIndex,
        std::move(hostsIndexes),
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
            auto location =
                VChunkConfig.GetPBufferLocation(pbufferResponse.HostIndex);
            if (!HasError(pbufferResponse.Error)) {
                CompletedWrites.Set(location);
            } else {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "OnWriteToManyPBuffersResponse error on location %d: %s",
                    location,
                    FormatError(pbufferResponse.Error).c_str());
            }
        }
    }

    if (CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount) {
        Reply(MakeError(S_OK));
        return;
    }

    const auto availableHandOffLocations = GetAvailableHandOffLocations();
    if (CompletedWrites.Count() + availableHandOffLocations.size() <
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
            "trying to send fallback writeRequest to %d handoff",
            i);

        SendWriteRequest(availableHandOffLocations[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
