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
// mainCandidates hosts have a priority, usually they are handoffs.
THostMask TakeNHosts(
    TVector<std::optional<THostIndex>> mainCandidates,
    THostMask& hosts,
    size_t n)
{
    Y_ASSERT(n > 0);
    Y_ASSERT(hosts.Count() >= n);
    THostMask res;

    for (size_t i = 0; i < mainCandidates.size() && res.Count() < n; ++i) {
        auto& host = mainCandidates[i];
        if (host && hosts.Get(*host)) {
            res.Set(*host);
            hosts.Reset(*host);
        }
    }
    while (res.Count() < n) {
        res.Set(*hosts.begin());
        hosts.Reset(*hosts.begin());
    }
    return res;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TWriteWithPbReplicationRequestExecutor::TWriteWithPbReplicationRequestExecutor(
    NActors::TActorSystem* actorSystem,
    TChildLogTitle logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    std::shared_ptr<TWriteRequestBundle> bundle)
    : TBaseWriteRequestExecutor(
          actorSystem,
          std::move(logTitle),
          vChunkConfig,
          directBlockGroup,
          std::move(bundle))
    , PbufferReplyTimeout(
          directBlockGroup->GetOracle()->GetPBufferReplyTimeout())
{
    AvailableHostsForDirectSending = VChunkConfig.GetDesiredPBuffers().Include(
        VChunkConfig.GetSecondaryPBuffers());
}

void TWriteWithPbReplicationRequestExecutor::Run()
{
    Bundle->GetSpan().Event("Run");
    ScheduleRequestTimeoutCallback();
    ScheduleHedging();

    SendWriteRequestToManyPBuffers(VChunkConfig.GetDesiredPBuffers().Hosts());
}

void TWriteWithPbReplicationRequestExecutor::SendWriteRequestToManyPBuffers(
    TVector<THostIndex> hosts)
{
    Y_ABORT_IF(IsAlreadyReplied());

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s SendWriteRequestToManyPBuffers",
        LogTitle.GetWithTime().c_str());

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

    DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.GetVChunkIndex(),
        coordinatorHostIndex,
        std::move(hosts),
        Bundle->GetLsn(),
        Bundle->GetVChunkRange(),
        PbufferReplyTimeout,
        Bundle->GetSgList(),
        Bundle->GetSpan().GetTraceId(),
        [self =
             std::static_pointer_cast<TWriteWithPbReplicationRequestExecutor>(
                 shared_from_this())](
            TDBGWriteBlocksToManyPBuffersResponse response)
        { self->OnWriteToManyPBuffersResponse(response); });
}

void TWriteWithPbReplicationRequestExecutor::OnWriteToManyPBuffersResponse(
    const TDBGWriteBlocksToManyPBuffersResponse& response)
{
    if (HasError(response.OverallError)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OnWriteToManyPBuffersResponse fatal: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response.OverallError).c_str());
        TryToSendDirectWrites(false);
        return;
    }

    THostMask completedWritesOfCurrentResponse;
    for (const auto& pbufferResponse: response.Responses) {
        const auto host = pbufferResponse.HostIndex;
        AvailableHostsForDirectSending.Reset(host);

        if (!HasError(pbufferResponse.Error)) {
            LOG_DEBUG(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s OnWriteToManyPBuffersResponse ok on %s",
                LogTitle.GetWithTime().c_str(),
                PrintHostIndex(host).c_str());

            completedWritesOfCurrentResponse.Set(host);
        } else {
            LOG_WARN(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s OnWriteToManyPBuffersResponse error on host %s: %s",
                LogTitle.GetWithTime().c_str(),
                PrintHostIndex(host).c_str(),
                FormatError(pbufferResponse.Error).c_str());
            // The error will be set and replied below.
        }
    }

    CompletedWrites = CompletedWrites.Include(completedWritesOfCurrentResponse);

    if (ShouldReplyOk()) {
        ReplyOrNotifyBelated(MakeError(S_OK), completedWritesOfCurrentResponse);
        return;
    }

    TryToSendDirectWrites(false);
}

void TWriteWithPbReplicationRequestExecutor::TryToSendDirectWrites(bool isHedge)
{
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteToManyPBuffersResponse isHedge: %s considering to send "
        "fallback",
        LogTitle.GetWithTime().c_str(),
        BoolToString(isHedge));

    bool needToSend = CompletedWrites.Count() + ActiveDirectWrites.Count() <
                      QuorumDirectBlockGroupHostCount;

    // We are relying on the IC layer: a reply will eventually arrive,
    // and requests will not hang forever.
    if (!needToSend) {
        return;
    }

    size_t neededRequestsNumber = QuorumDirectBlockGroupHostCount -
                                  CompletedWrites.Count() -
                                  ActiveDirectWrites.Count();
    bool haveEnoughAvailableHostsForSending =
        neededRequestsNumber <= AvailableHostsForDirectSending.Count();

    if (!haveEnoughAvailableHostsForSending) {
        auto resultError =
            MakeError(E_FAIL, "Direct additional requests are not available");
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OnWriteToManyPBuffersResponse isHedge: %s : %s",
            LogTitle.GetWithTime().c_str(),
            BoolToString(isHedge),
            FormatError(resultError).c_str());

        ReplyOrNotifyBelated(resultError, {});
        return;
    }

    TVector<std::optional<THostIndex>> mainCandidates = {
        VChunkConfig.GetSecondaryPBuffers().Nth(0),
        VChunkConfig.GetSecondaryPBuffers().Nth(1)};
    for (auto host: TakeNHosts(
             std::move(mainCandidates),
             AvailableHostsForDirectSending,
             neededRequestsNumber))
    {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OnWriteToManyPBuffersResponse isHedge: %s: trying to send "
            "fallback writeRequest to %s",
            LogTitle.GetWithTime().c_str(),
            BoolToString(isHedge),
            PrintHostIndex(host).c_str());

        SendDirectWriteRequest(host);
    }
}

void TWriteWithPbReplicationRequestExecutor::OnWriteResponse(
    THostIndex host,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteToManyPBuffersResponse DirectResponse on %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(host).c_str());

    ActiveDirectWrites.Reset(host);

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        if (ShouldReplyOk()) {
            ReplyOrNotifyBelated(MakeError(S_OK), THostMask::MakeOne(host));
        }
        return;
    }
    // There is no necessity of vchunk's notifying in case of error.
    // Notifying has sense only for CompletedWrites.
    // RequestedWrites will be registered in any case.

    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteToManyPBuffersResponse DirectResponse %s on %s",
        LogTitle.GetWithTime().c_str(),
        FormatError(response.Error).c_str(),
        PrintHostIndex(host).c_str());

    auto spanEnder = TEndSpanWithError(std::move(span), response.Error);

    TryToSendDirectWrites(false);
}

void TWriteWithPbReplicationRequestExecutor::ScheduleHedging()
{
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s SendWriteRequestToManyPBuffers: schedule hedge",
        LogTitle.GetWithTime().c_str());

    DirectBlockGroup->Schedule(
        HedgingDelay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = std::static_pointer_cast<
                    TWriteWithPbReplicationRequestExecutor>(weakSelf.lock()))
            {
                if (!self->IsAlreadyReplied()) {
                    self->TryToSendDirectWrites(true);
                }
            }
        });
}

void TWriteWithPbReplicationRequestExecutor::SendDirectWriteRequest(
    THostIndex host)
{
    ActiveDirectWrites.Set(host);
    SendWriteRequest(host);
}

TString TWriteWithPbReplicationRequestExecutor::ExtendedDebugState() const
{
    TStringBuilder result;
    result << TBaseWriteRequestExecutor::ExtendedDebugState();
    result << "AvailableHostsForDirectSending: "
           << AvailableHostsForDirectSending.Print() << ";";
    result << "ActiveDirectWrites: " << ActiveDirectWrites.Print() << ";";

    return result;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
