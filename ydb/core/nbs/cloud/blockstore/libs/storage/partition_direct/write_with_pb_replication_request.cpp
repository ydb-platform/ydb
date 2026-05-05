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
    ++PlannedRequests;
    SendWriteRequestToManyPBuffers(
        {ELocation::PBuffer0, ELocation::PBuffer1, ELocation::PBuffer2});
}

void TWriteWithPbReplicationRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "SendWriteRequestToManyPBuffers: schedule hedge %s, %s",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    ++PlannedRequests;
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
                         ELocation::HOPBuffer1},
                        true);
                }
            }
        });
}

void TWriteWithPbReplicationRequestExecutor::SendWriteRequestToManyPBuffers(
    TVector<ELocation> locations,
    bool isHedge)
{
    if (Promise.IsReady()) {
        return;
    }

    TVector<ui8> hostsIndexes;
    hostsIndexes.reserve(3);
    for (auto location: locations) {
        hostsIndexes.push_back(VChunkConfig.GetHostIndex(location));
    }
    RequestedWrites.Set(locations[0]);

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "SendWriteRequestToManyPBuffers: isHedge:[%d], %s, %s",
        isHedge,
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

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
    ++FinishedRequests;
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse: overall err: %s, %s, %s",
        FormatError(response.OverallError).c_str(),
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    if (HasError(response.OverallError)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse fatal error: %s, %s %s",
            FormatError(response.OverallError).c_str(),
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());
        // The error will be set and replied below.
    } else {
        for (const auto& pbufferResponse: response.Responses) {
            auto location =
                VChunkConfig.GetPBufferLocation(pbufferResponse.HostIndex);
            if (!HasError(pbufferResponse.Error)) {
                LOG_DEBUG(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "OnWriteToManyPBuffersResponse ok on location %d: %s %s %s",
                    location,
                    FormatError(pbufferResponse.Error).c_str(),
                    Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
                    Request->Headers.Range.Print().c_str());
                CompletedWrites.Set(location);
            } else {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "OnWriteToManyPBuffersResponse error on location %d: %s %s "
                    "%s",
                    location,
                    FormatError(pbufferResponse.Error).c_str(),
                    Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
                    Request->Headers.Range.Print().c_str());
            }
        }
    }

    if (CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount) {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse response with no single retries %s %s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());
        Reply(MakeError(S_OK));
        return;
    }

    //-----
    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse trying to send fallback writeRequest: "
        "%s, %s",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    const auto availableHandOffLocations = GetAvailableHandOffLocations();
    bool allWriteWithPbReplicationRespondsReceived =
        FinishedRequests == PlannedRequests;
    bool haveEnoughHandOffs =
        CompletedWrites.Count() + availableHandOffLocations.size() >=
        QuorumDirectBlockGroupHostCount;
    if (allWriteWithPbReplicationRespondsReceived && !haveEnoughHandOffs) {
        auto resultError =
            MakeError(E_FAIL, "Hand-offs retries are not available");
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse: %s, %s, %s",
            FormatError(resultError).c_str(),
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        Reply(resultError);
        return;
    }

    //--

    // Sending request to handoff in case of 1-2 errors
    for (size_t i = 0;
         i < QuorumDirectBlockGroupHostCount - CompletedWrites.Count();
         ++i)
    {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "trying to send fallback writeRequest to %d handoff location %s %s",
            availableHandOffLocations[i],
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        SendWriteRequest(availableHandOffLocations[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
