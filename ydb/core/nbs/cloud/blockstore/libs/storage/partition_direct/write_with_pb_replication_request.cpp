#include "write_with_pb_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/random/shuffle.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

const char* BoolToString(bool b)
{
    return b ? "true" : "false";
}

// @brief
// Method takes n hosts from 'hosts'.
// The code that calls this method must check work predicates by itself -
//   here we have asserts.
// mainCandidates hosts have a priority, usually there are handoffs.
TVector<THostIndex> TakeNHosts(
    TVector<std::optional<THostIndex>> mainCandidates,
    THostMask& hosts,
    size_t n)
{
    Y_ASSERT(n > 0);
    Y_ASSERT(hosts.Count() >= n);
    TVector<THostIndex> res;
    res.reserve(n);

    for (size_t i = 0; i < mainCandidates.size() && res.size() < n; ++i) {
        auto& host = mainCandidates[i];
        if (host && hosts.Get(*host)) {
            res.push_back(*host);
            hosts.Reset(*host);
        }
    }
    while (res.size() < n) {
        res.push_back(*hosts.begin());
        hosts.Reset(*hosts.begin());
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
    NWilson::TTraceId traceId)
    : TBaseWriteRequestExecutor(
          actorSystem,
          vChunkConfig,
          directBlockGroup,
          vChunkRange,
          std::move(callContext),
          std::move(request),
          lsn,
          std::move(traceId))
    , PbufferReplyTimeout(
          directBlockGroup->GetOracle()->GetPBufferReplyTimeout())
{
    const auto& pbufferHosts = VChunkConfig.PBufferHosts;
    AvailableHostsForDirectSending =
        pbufferHosts.GetPrimary().Include(pbufferHosts.GetHandOff());
}

void TWriteWithPbReplicationRequestExecutor::Run()
{
    ScheduleRequestTimeoutCallback();
    ScheduleHedging();

    SendWriteRequestToManyPBuffers(
        VChunkConfig.PBufferHosts.GetPrimary().Hosts());
}

void TWriteWithPbReplicationRequestExecutor::SendWriteRequestToManyPBuffers(
    TVector<THostIndex> hosts)
{
    if (Promise.IsReady()) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "SendWriteRequestToManyPBuffers %s %s",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    const THostIndex coordinatorHostIndex =
        DirectBlockGroup->GetOracle()->SelectBestPBufferHost(
            hosts,
            EOperation::WriteToManyPBuffers);

    // coordinatorHostIndex is a direct destination so we erase it from
    // future write attempts.
    AvailableHostsForDirectSending.Reset(coordinatorHostIndex);
    for (auto host: hosts) {
        RequestedWrites.Set(host);
    }

    auto future = DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.VChunkIndex,
        coordinatorHostIndex,
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
            "OnWriteToManyPBuffersResponse fatal error %s %s %s",
            FormatError(response.OverallError).c_str(),
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());
        TryToSendDirectWrites(false);
        return;
    }

    for (const auto& pbufferResponse: response.Responses) {
        const auto host = pbufferResponse.HostIndex;
        AvailableHostsForDirectSending.Reset(host);
        if (!HasError(pbufferResponse.Error)) {
            LOG_INFO(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "OnWriteToManyPBuffersResponse ok on host %d %s %s",
                host,
                Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
                Request->Headers.Range.Print().c_str());
            CompletedWrites.Set(host);
        } else {
            LOG_WARN(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "OnWriteToManyPBuffersResponse error on host %d: %s %s %s",
                host,
                FormatError(pbufferResponse.Error).c_str(),
                Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
                Request->Headers.Range.Print().c_str());
            // The error will be set and replied below.
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
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse isHedge: %s considering to send fallback"
        " %s %s",
        BoolToString(isHedge),
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    bool needToSend = CompletedWrites.Count() + ActiveDirectWritesNumber <
                      QuorumDirectBlockGroupHostCount;
    if (!needToSend) {
        return;
    }

    size_t neededRequestsNumber = QuorumDirectBlockGroupHostCount -
                                  CompletedWrites.Count() -
                                  ActiveDirectWritesNumber;
    bool haveEnoughAvailableHostsForSending =
        neededRequestsNumber <= AvailableHostsForDirectSending.Count();

    if (!haveEnoughAvailableHostsForSending) {
        auto resultError =
            MakeError(E_FAIL, "Direct additional requests are not available");
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse isHedge: %s : %s %s %s",
            BoolToString(isHedge),
            FormatError(resultError).c_str(),
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        Reply(resultError);
        return;
    }

    TVector<std::optional<THostIndex>> mainCandidates = {
        VChunkConfig.PBufferHosts.GetHandOff().Nth(0),
        VChunkConfig.PBufferHosts.GetHandOff().Nth(1)};
    for (auto host: TakeNHosts(
             std::move(mainCandidates),
             AvailableHostsForDirectSending,
             neededRequestsNumber))
    {
        LOG_INFO(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse isHedge: %s: trying to send "
            "fallback writeRequest to %d host %s %s",
            BoolToString(isHedge),
            host,
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        SendDirectWriteRequest(host);
    }
}

void TWriteWithPbReplicationRequestExecutor::OnWriteResponse(
    THostIndex host,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse DirectResponse on %d host %s %s",
        host,
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    --ActiveDirectWritesNumber;
    if (Promise.IsReady()) {
        return;
    }

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        if (ShouldReplyOk()) {
            Reply(MakeError(S_OK));
        }
        return;
    }

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "OnWriteToManyPBuffersResponse DirectResponse error on %d host %s"
        " %s %s",
        host,
        FormatError(response.Error).c_str(),
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    auto spanEnder = TEndSpanWithError(std::move(span), response.Error);

    TryToSendDirectWrites(false);
}

void TWriteWithPbReplicationRequestExecutor::ScheduleHedging()
{
    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "SendWriteRequestToManyPBuffers: schedule hedge %s %s",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    // const auto hedgingDelay = PbufferReplyTimeout * 0.9;
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
    THostIndex host)
{
    ++ActiveDirectWritesNumber;
    SendWriteRequest(host);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
