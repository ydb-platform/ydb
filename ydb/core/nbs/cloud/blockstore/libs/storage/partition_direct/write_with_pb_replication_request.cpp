#include "write_with_pb_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define PBLOG(priority, fmt, ...)                              \
    LOG_##priority(                                            \
        *ActorSystem,                                          \
        NKikimrServices::NBS_PARTITION,                        \
        fmt " %s %s" __VA_OPT__(, ) __VA_ARGS__,               \
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(), \
        Request->Headers.Range.Print().c_str())

#define PBLOG_DEBUG(fmt, ...) PBLOG(DEBUG, fmt __VA_OPT__(, ) __VA_ARGS__)
#define PBLOG_INFO(fmt, ...) PBLOG(INFO, fmt __VA_OPT__(, ) __VA_ARGS__)
#define PBLOG_WARN(fmt, ...) PBLOG(WARN, fmt __VA_OPT__(, ) __VA_ARGS__)
#define PBLOG_ERROR(fmt, ...) PBLOG(ERROR, fmt __VA_OPT__(, ) __VA_ARGS__)

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

const char* BoolToString(bool b)
{
    return b ? "true" : "false";
}

// @brief
// Method takes n locations from 'locations'.
// The code that calls this method must check work predicates by itself -
//   here we have asserts.
// Handoff locations have a priority.
TVector<ELocation> TakeNLocations(TLocationMask& locations, size_t n)
{
    Y_ASSERT(n > 0);
    Y_ASSERT(locations.Count() >= n);
    const TVector<ELocation> mainCandidates = {
        ELocation::HOPBuffer0,
        ELocation::HOPBuffer1};
    TVector<ELocation> res;
    res.reserve(n);

    for (size_t i = 0; i < mainCandidates.size() && res.size() < n; ++i) {
        if (locations.Get(mainCandidates[i])) {
            res.push_back(mainCandidates[i]);
            locations.Reset(mainCandidates[i]);
        }
    }
    while (res.size() < n) {
        res.push_back(*locations.begin());
        locations.Reset(*locations.begin());
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
    AvailableLocationsForDirectSending = TLocationMask::MakeAllPBuffers();
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

    // first location is direct destination so we erase it from future write
    // attempts.
    AvailableLocationsForDirectSending.Reset(locations[0]);
    for (auto location: locations) {
        hostsIndexes.push_back(VChunkConfig.GetHostIndex(location));
        RequestedWrites.Set(location);
    }

    PBLOG_DEBUG("SendWriteRequestToManyPBuffers");

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
        PBLOG_ERROR(
            "OnWriteToManyPBuffersResponse fatal error %s",
            FormatError(response.OverallError).c_str());
        TryToSendDirectWrites(false);
        return;
    }

    for (const auto& pbufferResponse: response.Responses) {
        auto location =
            VChunkConfig.GetPBufferLocation(pbufferResponse.HostIndex);
        AvailableLocationsForDirectSending.Reset(location);
        if (!HasError(pbufferResponse.Error)) {
            PBLOG_DEBUG(
                "OnWriteToManyPBuffersResponse ok on location %s",
                ToString(location).c_str());
            CompletedWrites.Set(location);
        } else {
            PBLOG_INFO(
                "OnWriteToManyPBuffersResponse error on location %s: %s",
                ToString(location).c_str(),
                FormatError(pbufferResponse.Error).c_str());
        }
    }

    if (ShouldReplyOk()) {
        Reply(MakeError(S_OK));
        return;
    }

    TryToSendDirectWrites(false);
}

void TWriteWithPbReplicationRequestExecutor::TryToSendDirectWrites(bool isHedge)
{
    PBLOG_INFO(
        "OnWriteToManyPBuffersResponse isHedge:%s considering to send fallback",
        BoolToString(isHedge));

    bool needToSend = CompletedWrites.Count() + ActiveDirectWritesNumber <
                      QuorumDirectBlockGroupHostCount;
    if (!needToSend) {
        return;
    }

    size_t neededRequestsNumber = QuorumDirectBlockGroupHostCount -
                                  CompletedWrites.Count() -
                                  ActiveDirectWritesNumber;
    bool haveEnoughAvailableLocationsForSending =
        neededRequestsNumber <= AvailableLocationsForDirectSending.Count();

    if (!haveEnoughAvailableLocationsForSending) {
        auto resultError =
            MakeError(E_FAIL, "Direct additional requests are not available");
        PBLOG_ERROR(
            "OnWriteToManyPBuffersResponse isHedge:%s : %s",
            BoolToString(isHedge),
            FormatError(resultError).c_str());

        Reply(resultError);
        return;
    }

    for (auto location: TakeNLocations(
             AvailableLocationsForDirectSending,
             neededRequestsNumber))
    {
        PBLOG_INFO(
            "OnWriteToManyPBuffersResponse isHedge: %s: trying to send "
            "fallback writeRequest to %d location",
            BoolToString(isHedge),
            location);

        SendDirectWriteRequest(location);
    }
}

void TWriteWithPbReplicationRequestExecutor::OnWriteResponse(
    ELocation location,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    PBLOG_INFO(
        "OnWriteToManyPBuffersResponse DirectResponse on %d location",
        location);

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

    PBLOG_WARN(
        "OnWriteToManyPBuffersResponse DirectResponse error on %d location %s",
        location,
        FormatError(response.Error).c_str());

    auto spanEnder = TEndSpanWithError(std::move(span), response.Error);

    TryToSendDirectWrites(false);
}

void TWriteWithPbReplicationRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    PBLOG_INFO("SendWriteRequestToManyPBuffers: schedule hedge");

    DirectBlockGroup->Schedule(
        HedgingDelay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = std::static_pointer_cast<
                    TWriteWithPbReplicationRequestExecutor>(weakSelf.lock()))
            {
                if (!self->Promise.IsReady()) {
                    self->TryToSendDirectWrites(true);
                }
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

#undef PBLOG_ERROR
#undef PBLOG_WARN
#undef PBLOG_INFO
#undef PBLOG_DEBUG
#undef PBLOG
