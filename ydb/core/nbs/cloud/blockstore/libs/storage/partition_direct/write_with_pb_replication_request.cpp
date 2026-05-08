#include "write_with_pb_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

void EraseLocationIfExists(
    TSet<ELocation>& allLocations,
    ELocation deletingLocation)
{
    auto it = allLocations.find(deletingLocation);
    if (it != allLocations.end()) {
        allLocations.erase(it);
    }
}

TVector<ELocation> TakeNLocations(TSet<ELocation>& locations, ui32 n)
{
    Y_ASSERT(n > 0);
    Y_ASSERT(locations.size() >= n);
    const TVector<ELocation> mainCandidates = {
        ELocation::HOPBuffer0,
        ELocation::HOPBuffer1};
    TVector<ELocation> res;
    res.reserve(n);

    for (size_t i = 0; i < mainCandidates.size() && res.size() < n; ++i) {
        auto it = locations.find(mainCandidates[i]);
        if (it != locations.end()) {
            res.push_back(*it);
            locations.erase(it);
        }
    }
    while (res.size() < n) {
        res.push_back(*locations.begin());
        locations.erase(locations.begin());
    }
    return res;
}

}   // namespace

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
{
    AvailableLocationsForDirectSending = {
        ELocation::PBuffer0,
        ELocation::PBuffer1,
        ELocation::PBuffer2,
        ELocation::HOPBuffer0,
        ELocation::HOPBuffer1};
}

void TWriteWithPbReplicationRequestExecutor::Run()
{
    ScheduleRequestTimeoutCallback();
    ScheduleHedging();
    SendWriteRequestToManyPBuffers(
        {ELocation::PBuffer0, ELocation::PBuffer1, ELocation::PBuffer2});
}

void TWriteWithPbReplicationRequestExecutor::SendWriteRequestToManyPBuffers(
    TVector<ELocation> locations)
{
    if (Promise.IsReady()) {
        return;
    }

    TVector<ui8> hostsIndexes;
    hostsIndexes.reserve(3);

    EraseLocationIfExists(AvailableLocationsForDirectSending, locations[0]);
    for (auto location: locations) {
        hostsIndexes.push_back(VChunkConfig.GetHostIndex(location));
        RequestedWrites.Set(location);
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "SendWriteRequestToManyPBuffers, %s, %s",
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
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse: overall err: %s, %s, %s, num responses "
        "inside: %d",
        FormatError(response.OverallError).c_str(),
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str(),
        response.Responses.size());

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
            EraseLocationIfExists(AvailableLocationsForDirectSending, location);
        }
    }

    if (ShouldReplyOk()) {
        Reply(MakeError(S_OK));
        return;
    }

    TryToSendDirectWrites();
}

void TWriteWithPbReplicationRequestExecutor::TryToSendDirectWrites(bool isHedge)
{
    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse isHedge:%d considering to send fallback "
        "writeRequest: %s, %s",
        isHedge,
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    bool needToSend = CompletedWrites.Count() + ActiveDirectWritesNumber <
                      QuorumDirectBlockGroupHostCount;
    if (!needToSend) {
        return;
    }

    ui32 neededRequestsNumber = QuorumDirectBlockGroupHostCount -
                                CompletedWrites.Count() -
                                ActiveDirectWritesNumber;
    bool haveEnoughHandOffs =
        neededRequestsNumber <= AvailableLocationsForDirectSending.size();

    if (!haveEnoughHandOffs) {
        auto resultError =
            MakeError(E_FAIL, "Hand-offs retries are not available");
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse isHedge:%d : %s, %s, %s",
            isHedge,
            FormatError(resultError).c_str(),
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        Reply(resultError);
        return;
    }

    for (auto location: TakeNLocations(
             AvailableLocationsForDirectSending,
             neededRequestsNumber))
    {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse isHedge[%d]: trying to send "
            "fallback writeRequest to %d handoff location %s %s",
            isHedge,
            location,
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        SendDirectWriteRequest(location);
    }
}

void TWriteWithPbReplicationRequestExecutor::OnWriteResponse(
    ELocation location,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse OnWriteResponse %d handoff location %s "
        "%s",
        location,
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    --ActiveDirectWritesNumber;
    if (Promise.IsReady()) {
        return;
    }

    if (!HasError(response.Error)) {
        CompletedWrites.Set(location);
        if (ShouldReplyOk()) {
            Reply(MakeError(S_OK));
        }
        return;
    }

    LOG_ERROR(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse OnWriteResponse %d handoff location %s "
        "%s error %s",
        location,
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str(),
        FormatError(response.Error).c_str());

    auto ender = TEndSpanWithError(std::move(span), response.Error);

    TryToSendDirectWrites();
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

    DirectBlockGroup->Schedule(
        HedgingDelay,
        [self =
             std::static_pointer_cast<TWriteWithPbReplicationRequestExecutor>(
                 shared_from_this())]()
        {
            if (!self->Promise.IsReady()) {
                self->TryToSendDirectWrites(true);
            }
        });
}

void TWriteWithPbReplicationRequestExecutor::SendDirectWriteRequest(
    ELocation location)
{
    ++ActiveDirectWritesNumber;
    SendWriteRequest(location);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
