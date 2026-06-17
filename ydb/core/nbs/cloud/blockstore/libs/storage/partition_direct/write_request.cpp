#include "write_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteRequestExecutor::TWriteRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    std::shared_ptr<TWriteRequestBundle> bundle)
    : ActorSystem(actorSystem)
    , WriteMode(directBlockGroup->GetOracle()->GetWriteMode())
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"t", ToString(WriteMode)},
           {"r", bundle->GetVChunkRange().Print()}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Bundle(std::move(bundle))
    , HedgingDelay(DirectBlockGroup->GetOracle()->GetWriteHedgingDelay())
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetWriteRequestTimeout())
    , IndirectWriteReplyTimeout(
          DirectBlockGroup->GetOracle()->GetPBufferReplyTimeout())
{
    Y_ABORT_UNLESS(
        VChunkConfig.GetDesiredPBuffers().Count() >=
        QuorumDirectBlockGroupHostCount);
}

TWriteRequestExecutor::~TWriteRequestExecutor()
{
    if (!IsReplied) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply not sent. %s",
            LogTitle.GetWithTime().c_str(),
            ExtendedDebugState().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TWriteRequestExecutor::Run()
{
    Bundle->GetSpan().Event("Run");
    ScheduleRequestTimeout();
    ScheduleHedging();

    switch (WriteMode) {
        case EWriteMode::PBufferReplication: {
            SendIndirectWriteRequest();
            break;
        }
        case EWriteMode::DirectPBuffersFilling: {
            for (auto host: VChunkConfig.GetDesiredPBuffers()) {
                SendDirectWriteRequest(host);
            }
            break;
        }
    }
}

TString TWriteRequestExecutor::Print()
{
    TStringBuilder result;
    result << LogTitle.GetWithTime() << " " << ExtendedDebugState() << " "
           << (IsReplied ? "Replied" : "Not replied");

    return result;
}

void TWriteRequestExecutor::SendIndirectWriteRequest()
{
    RequestedIndirectWrites = VChunkConfig.GetDesiredPBuffers();
    auto hosts = RequestedIndirectWrites.Hosts();

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s SendIndirectWriteRequest %s",
        LogTitle.GetWithTime().c_str(),
        RequestedIndirectWrites.Print().c_str());

    auto coordinator = DirectBlockGroup->GetOracle()->SelectBestPBufferHost(
        hosts,
        EOperation::WriteToManyPBuffers);
    IndirectCoordinator.Set(coordinator);

    DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.GetVChunkIndex(),
        coordinator,
        std::move(hosts),
        Bundle->GetLsn(),
        Bundle->GetVChunkRange(),
        IndirectWriteReplyTimeout,
        Bundle->GetSgList(),
        Bundle->GetSpan().GetTraceId(),
        [self = shared_from_this()]   //
        (const TDBGWriteBlocksToManyPBuffersResponse& response)
        {
            self->OnIndirectWriteResponse(response);   //
        });
}

void TWriteRequestExecutor::OnIndirectWriteResponse(
    const TDBGWriteBlocksToManyPBuffersResponse& response)
{
    if (HasError(response.OverallError)) {
        FailedWrites = FailedWrites.Include(IndirectCoordinator);

        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OnIndirectWriteResponse: %s %s",
            LogTitle.GetWithTime().c_str(),
            ExtendedDebugState().c_str(),
            FormatError(response.OverallError).c_str());

        SendAdditionalDirectWrites();
        return;
    }

    THostMask completedWritesOfCurrentResponse;
    for (const auto& pbufferResponse: response.Responses) {
        const auto host = pbufferResponse.HostIndex;

        if (!HasError(pbufferResponse.Error)) {
            LOG_DEBUG(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s OnIndirectWriteResponse %s OK",
                LogTitle.GetWithTime().c_str(),
                PrintHostIndex(host).c_str());

            completedWritesOfCurrentResponse.Set(host);
        } else {
            LOG_WARN(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "%s OnIndirectWriteResponse %s %s",
                LogTitle.GetWithTime().c_str(),
                PrintHostIndex(host).c_str(),
                FormatError(pbufferResponse.Error).c_str());

            FailedWrites.Set(host);
            // The error will be set and replied below.
        }
    }

    CompletedWrites = CompletedWrites.Include(completedWritesOfCurrentResponse);

    if (ShouldReplyOk()) {
        ReplyOrNotifyBelated(MakeError(S_OK), completedWritesOfCurrentResponse);
        return;
    }

    SendAdditionalDirectWrites();
}

void TWriteRequestExecutor::SendAdditionalDirectWrites()
{
    if (IsReplied) {
        return;
    }

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s SendAdditionalDirectWrites %s",
        LogTitle.GetWithTime().c_str(),
        ExtendedDebugState().c_str());

    if (!IsQuorumReachable()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Quorum unreachable. %s",
            LogTitle.GetWithTime().c_str(),
            ExtendedDebugState().c_str());
        Reply(MakeError(E_FAIL, "Quorum unreachable " + ExtendedDebugState()));
        return;
    }

    if (GetRunningDirectWrites().Count() >= GetQuorumDeficit()) {
        // Don't need to run more direct writes.
        // The running direct writes is enough to reach a quorum.
        return;
    }

    // We will try to get the required number of successful responses by sending
    // direct writes to handoffs and hosts to which requests were sent
    // indirectly.

    // Additional direct writes for handoffs.
    SendDirectWriteRequestsToHandoffs(
        GetQuorumDeficit() - GetRunningDirectWrites().Count());

    // Additional direct writes for desired when needed
    SendDirectWriteRequestsToDesired(
        GetQuorumDeficit() - GetRunningDirectWrites().Count());
}

void TWriteRequestExecutor::SendDirectWriteRequestsToDesired(size_t count)
{
    if (IsReplied || !count) {
        return;
    }

    const auto hosts = VChunkConfig.GetDesiredPBuffers()
                           .Exclude(RequestedDirectWrites)
                           .Exclude(CompletedWrites)
                           .Exclude(FailedWrites)
                           .Exclude(IndirectCoordinator);

    for (THostIndex host: hosts) {
        SendDirectWriteRequest(host);
        if (--count == 0) {
            break;
        }
    }
}

void TWriteRequestExecutor::SendDirectWriteRequestsToHandoffs(size_t count)
{
    if (IsReplied || !count) {
        return;
    }

    const auto hosts = VChunkConfig.GetSecondaryPBuffers()
                           .Exclude(RequestedDirectWrites)
                           .Exclude(CompletedWrites)
                           .Exclude(FailedWrites)
                           .Exclude(IndirectCoordinator);

    for (THostIndex host: hosts) {
        SendDirectWriteRequest(host);
        if (--count == 0) {
            break;
        }
    }
}

void TWriteRequestExecutor::SendDirectWriteRequest(THostIndex host)
{
    if (IsReplied) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Send DirectWriteRequest to %s %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(host).c_str(),
        ExtendedDebugState().c_str());

    auto span = DirectBlockGroup->CreateChildSpan(
        Bundle->GetSpan().GetTraceId(),
        "TWriteRequestExecutor");
    if (span) {
        span->Attribute("HostIndex", ToString(host));
    }

    RequestedDirectWrites.Set(host);

    auto future = DirectBlockGroup->WriteBlocksToPBuffer(
        VChunkConfig.GetVChunkIndex(),
        host,
        Bundle->GetLsn(),
        Bundle->GetVChunkRange(),
        Bundle->GetSgList(),
        span ? span->GetTraceId() : NWilson::TTraceId());

    future.Subscribe(
        [self = shared_from_this(), host, span = std::move(span)]           //
        (const NThreading::TFuture<TDBGWriteBlocksResponse>& f) mutable {   //
            self->OnDirectWriteResponse(host, f.GetValue(), std::move(span));
        });
}

void TWriteRequestExecutor::OnDirectWriteResponse(
    THostIndex host,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnDirectWriteResponse %s %s",
        LogTitle.GetWithTime().c_str(),
        PrintHostIndex(host).c_str(),
        FormatError(response.Error).c_str());

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        if (ShouldReplyOk()) {
            ReplyOrNotifyBelated(MakeError(S_OK), THostMask::MakeOne(host));
        }
        return;
    }

    FailedWrites.Set(host);
    auto ender = TEndSpanWithError(std::move(span), response.Error);

    if (IsReplied) {
        return;
    }

    if (!IsQuorumReachable()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s It is impossible to reach a quorum. %s %s",
            LogTitle.GetWithTime().c_str(),
            ExtendedDebugState().c_str(),
            FormatError(response.Error).c_str());
        Reply(response.Error);
        return;
    }

    const auto candidates = VChunkConfig.GetDesiredPBuffers()
                                .Include(VChunkConfig.GetSecondaryPBuffers())
                                .Exclude(RequestedDirectWrites)
                                .Exclude(FailedWrites)
                                .Exclude(CompletedWrites)
                                .Exclude(IndirectCoordinator);

    if (candidates.Empty()) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s All hand-offs attempts are over. %s %s",
            LogTitle.GetWithTime().c_str(),
            ExtendedDebugState().c_str(),
            FormatError(response.Error).c_str());
        return;
    }

    SendDirectWriteRequest(*candidates.First());
}

void TWriteRequestExecutor::ReplyOrNotifyBelated(
    NProto::TError error,
    THostMask completedOnCurrentResponse)
{
    if (!IsReplied) {
        Reply(std::move(error));
        return;
    }
    NotifyBelated(completedOnCurrentResponse);
}

void TWriteRequestExecutor::Reply(NProto::TError error)
{
    Y_ABORT_IF(IsReplied, "TWriteRequestExecutor::Reply called twice");
    IsReplied = true;

    if (HasError(error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply error %s %s",
            LogTitle.GetWithTime().c_str(),
            ExtendedDebugState().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply OK.",
            LogTitle.GetWithTime().c_str());
    }

    Bundle->Reply(
        std::move(error),
        RequestedDirectWrites.Include(RequestedIndirectWrites),
        CompletedWrites);
}

void TWriteRequestExecutor::NotifyBelated(THostMask completedOnCurrentResponse)
{
    if (completedOnCurrentResponse.Empty()) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s NotifyBelated %s %s",
        LogTitle.GetWithTime().c_str(),
        ExtendedDebugState().c_str(),
        completedOnCurrentResponse.Print().c_str());

    Bundle->NotifyBelated(completedOnCurrentResponse);
}

void TWriteRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Schedule OnHedgingTimeout() %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(HedgingDelay).c_str());

    DirectBlockGroup->Schedule(
        HedgingDelay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->OnHedgingTimeout();
            }
        });
}

void TWriteRequestExecutor::ScheduleRequestTimeout()
{
    if (!RequestTimeout) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Schedule OnRequestTimeout() %s",
        LogTitle.GetWithTime().c_str(),
        FormatDuration(RequestTimeout).c_str());

    DirectBlockGroup->Schedule(
        RequestTimeout,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->OnRequestTimeout();
            }
        });
}

void TWriteRequestExecutor::OnHedgingTimeout()
{
    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnHedgingTimeout %s",
        LogTitle.GetWithTime().c_str(),
        ExtendedDebugState().c_str());

    switch (WriteMode) {
        case EWriteMode::PBufferReplication: {
            SendAdditionalDirectWrites();
            break;
        }
        case EWriteMode::DirectPBuffersFilling: {
            SendDirectWriteRequestsToHandoffs(GetQuorumDeficit());
            break;
        }
    }
}

void TWriteRequestExecutor::OnRequestTimeout()
{
    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Request timeout.",
        LogTitle.GetWithTime().c_str());

    ReplyOrNotifyBelated(MakeError(E_TIMEOUT, "Write request timeout"), {});
}

bool TWriteRequestExecutor::ShouldReplyOk() const
{
    return CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount;
}

bool TWriteRequestExecutor::IsQuorumReachable() const
{
    auto maybeSuccess = VChunkConfig.GetDesiredPBuffers()
                            .Include(VChunkConfig.GetSecondaryPBuffers())
                            .Exclude(FailedWrites);

    return maybeSuccess.Count() >= QuorumDirectBlockGroupHostCount;
}

size_t TWriteRequestExecutor::GetQuorumDeficit() const
{
    const size_t completedCount = CompletedWrites.Count();
    if (completedCount >= QuorumDirectBlockGroupHostCount) {
        return 0;
    }
    return QuorumDirectBlockGroupHostCount - completedCount;
}

THostMask TWriteRequestExecutor::GetRunningDirectWrites() const
{
    return RequestedDirectWrites.Exclude(CompletedWrites).Exclude(FailedWrites);
}

TString TWriteRequestExecutor::ExtendedDebugState() const
{
    TStringBuilder result;
    result << "dr:" << RequestedDirectWrites.Print();
    result << " ir:" << IndirectCoordinator.Print()
           << RequestedIndirectWrites.Print();
    result << " c:" << CompletedWrites.Print();
    result << " f:" << FailedWrites.Print();
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequestExecutorPtr CreateWriteRequestExecutor(
    NActors::TActorSystem* const actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    std::shared_ptr<TWriteRequestBundle> bundle)
{
    return std::make_shared<TWriteRequestExecutor>(
        actorSystem,
        logTitle,
        vChunkConfig,
        std::move(directBlockGroup),
        bundle);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
