#include "write_with_pb_replication_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/random/shuffle.h>

static ui64 RequestCount = 0;
static ui64 ReplyCount = 0;

#define PBLOG(priority, fmt, ...)                              \
    LOG_##priority(                                            \
        *ActorSystem,                                          \
        NKikimrServices::NBS_PARTITION,                        \
        fmt " %s %s" __VA_OPT__(, ) __VA_ARGS__,               \
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(), \
        Request->Headers.Range.Print().c_str());

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
    ++RequestCount;
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

    PBLOG_DEBUG("SendWriteRequestToManyPBuffers");

    // first host is direct destination so we erase it from future write
    // attempts.
    AvailableHostsForDirectSending.Reset(hosts[0]);
    for (auto host: hosts) {
        RequestedWrites.Set(host);
    }

    {
        THostMask mask;
        std::string strHosts;
        for (auto host: hosts) {
            mask.Set(host);
            strHosts += std::to_string(host) + " ";
        }
        PBLOG_DEBUG(
            "SendWriteRequestToManyPBuffers: hosts_masks: Hosts mask: '%s', "
            "strHosts: '%s', AvailableHostsForDirectSending: '%s', "
            "primary mask: '%s', GetHandOff: '%s'",
            mask.Print().c_str(),
            strHosts.c_str(),
            AvailableHostsForDirectSending.Print().c_str(),
            VChunkConfig.PBufferHosts.GetPrimary().Print().c_str(),
            VChunkConfig.PBufferHosts.GetHandOff().Print().c_str());
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
        PBLOG_ERROR(
            "OnWriteToManyPBuffersResponse fatal error %s",
            FormatError(response.OverallError).c_str());
        TryToSendDirectWrites(false);
        return;
    }

    for (const auto& pbufferResponse: response.Responses) {
        const auto host = pbufferResponse.HostIndex;
        AvailableHostsForDirectSending.Reset(host);
        if (!HasError(pbufferResponse.Error)) {
            PBLOG_INFO("OnWriteToManyPBuffersResponse ok on host %d", host);
            CompletedWrites.Set(host);
        } else {
            PBLOG_WARN(
                "OnWriteToManyPBuffersResponse error on host %d: %s",
                host,
                FormatError(pbufferResponse.Error).c_str());
            // The error will be set and replied below.
        }
    }

    if (ShouldReplyOk()) {
        MyReply(MakeError(S_OK));
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
    bool haveEnoughAvailableHostsForSending =
        neededRequestsNumber <= AvailableHostsForDirectSending.Count();

    if (!haveEnoughAvailableHostsForSending) {
        auto resultError =
            MakeError(E_FAIL, "Direct additional requests are not available");
        PBLOG_ERROR(
            "OnWriteToManyPBuffersResponse isHedge:%s : %s",
            BoolToString(isHedge),
            FormatError(resultError).c_str());

        MyReply(resultError);
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
        PBLOG_INFO(
            "OnWriteToManyPBuffersResponse isHedge: %s: trying to send "
            "fallback writeRequest to %d host",
            BoolToString(isHedge),
            host);

        SendDirectWriteRequest(host);
    }
}

void TWriteWithPbReplicationRequestExecutor::OnWriteResponse(
    THostIndex host,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    PBLOG_INFO("OnWriteToManyPBuffersResponse DirectResponse on %d host", host);

    --ActiveDirectWritesNumber;
    if (Promise.IsReady()) {
        return;
    }

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        if (ShouldReplyOk()) {
            MyReply(MakeError(S_OK));
        }
        return;
    }

    PBLOG_WARN(
        "OnWriteToManyPBuffersResponse DirectResponse error on %d host %s",
        host,
        FormatError(response.Error).c_str());

    auto spanEnder = TEndSpanWithError(std::move(span), response.Error);

    TryToSendDirectWrites(false);
}

void TWriteWithPbReplicationRequestExecutor::ScheduleHedging()
{
    PBLOG_INFO("SendWriteRequestToManyPBuffers: schedule hedge");

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

void TWriteWithPbReplicationRequestExecutor::MyReply(NProto::TError error)
{
    if (HasError(error)) {
        PBLOG_ERROR(
            "TBaseWriteRequestExecutor::Reply error: %s",
            FormatError(error).c_str());
    } else {
        PBLOG_DEBUG("TBaseWriteRequestExecutor::Reply");
    }

    if (Promise.IsReady()) {
        return;
    }

    Reply(std::move(error));
    ++ReplyCount;

    if (RequestCount % 10000 == 0 && RequestCount - ReplyCount > 32) {
        PBLOG_WARN(
            "TWriteWithPbReplicationRequestExecutor ololo 'RequestCount %d != "
            "%d ReplyCount'",
            RequestCount,
            ReplyCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect

#undef PBLOG_ERROR
#undef PBLOG_WARN
#undef PBLOG_INFO
#undef PBLOG_DEBUG
#undef PBLOG
