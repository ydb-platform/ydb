#include "write_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <util/string/cast.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SlowRequestTime = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

}   // namespace

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
           {"pBufferKey", bundle->GetPBufferKey().Print()},
           {"r", bundle->GetRange().Print()},
           {"rv", bundle->GetVChunkRange().Print()}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Bundle(std::move(bundle))
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetWriteRequestTimeout())
    , IndirectWriteReplyTimeout(
          DirectBlockGroup->GetOracle()->GetIndirectWriteReplyTimeout())
{}

TWriteRequestExecutor::~TWriteRequestExecutor()
{
    if (!IsReplied) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "Reply not sent",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

        Y_ABORT_UNLESS(false);
    }
}

void TWriteRequestExecutor::Run()
{
    StartAt = TInstant::Now();
    Bundle->GetSpan().Event("Run");

    const auto hosts = VChunkConfig.GetDesiredPBuffers();
    if (hosts.Count() < QuorumDirectBlockGroupHostCount) {
        Reply(MakeError(E_REJECTED, "Not enough PBuffer hosts"));
        return;
    }

    ScheduleRequestTimeout();
    ScheduleHedging(DirectBlockGroup->GetOracle()->GetWriteHedgingDelay(
        hosts,
        WriteMode == EWriteMode::IndirectWrite));

    switch (WriteMode) {
        case EWriteMode::IndirectWrite: {
            SendIndirectWriteRequest(hosts);
            break;
        }
        case EWriteMode::DirectWrite: {
            for (auto host: hosts) {
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

void TWriteRequestExecutor::SendIndirectWriteRequest(THostMask hosts)
{
    RequestedIndirectWrites = hosts;

    YDB_LOG_DEBUG_CTX(*ActorSystem, "SendIndirectWriteRequest",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_RequestedIndirectWrites.Print().c_str", RequestedIndirectWrites.Print()});

    auto coordinator = DirectBlockGroup->GetOracle()->SelectBestPBufferHost(
        hosts,
        EOperation::WriteToManyPBuffers);
    IndirectCoordinator.Set(coordinator);

    DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.GetVChunkIndex(),
        coordinator,
        hosts,
        Bundle->GetPBufferKey(),
        Bundle->GetVChunkRange(),
        IndirectWriteReplyTimeout,
        Bundle->GetSgList(),
        TBlockChecksums(),
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
    THostMask completedWritesOfCurrentResponse;
    for (const auto& pbufferResponse: response.Responses) {
        const auto host = pbufferResponse.HostIndex;

        if (!HasError(pbufferResponse.Error)) {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "OnIndirectWriteResponse OK",
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"#_PrintHostAndNode(host).c_str", PrintHostAndNode(host)});

            completedWritesOfCurrentResponse.Set(host);
        } else {
            YDB_LOG_WARN_CTX(*ActorSystem, "OnIndirectWriteResponse",
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"#_PrintHostAndNode(host).c_str", PrintHostAndNode(host)},
                {"#_FormatError(pbufferResponse.Error).Quote().c_str", FormatError(pbufferResponse.Error)});

            FailedWrites.Set(host);
            // The error will be set and replied below.
        }
    }

    CompletedWrites = CompletedWrites.Include(completedWritesOfCurrentResponse);

    MaybeReplyOrNotifyBelated(completedWritesOfCurrentResponse);
    MaybeSendAdditionalDirectWrites();
}

void TWriteRequestExecutor::MaybeSendAdditionalDirectWrites()
{
    if (IsReplied) {
        return;
    }

    YDB_LOG_TRACE_CTX(*ActorSystem, "MaybeSendAdditionalDirectWrites",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

    if (!IsQuorumReachable()) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "Quorum unreachable",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});
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
    Y_DEBUG_ABORT_UNLESS(!IsReplied);
    if (!count) {
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
    Y_DEBUG_ABORT_UNLESS(!IsReplied);
    if (!count) {
        return;
    }

    const auto hosts = VChunkConfig.GetSecondaryPBuffers()
                           .Exclude(RequestedDirectWrites)
                           .Exclude(CompletedWrites)
                           .Exclude(FailedWrites)
                           .Exclude(IndirectCoordinator);

    for (THostIndex host: hosts) {
        YDB_LOG_TRACE_CTX(*ActorSystem, "SendDirectWriteRequestsToHandoffs",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

        SendDirectWriteRequest(host);
        if (--count == 0) {
            break;
        }
    }
}

void TWriteRequestExecutor::SendDirectWriteRequest(THostIndex host)
{
    Y_DEBUG_ABORT_UNLESS(!IsReplied);

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Send DirectWriteRequest",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_PrintHostAndNode(host).c_str", PrintHostAndNode(host)},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

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
        Bundle->GetPBufferKey(),
        Bundle->GetVChunkRange(),
        Bundle->GetSgList(),
        TBlockChecksums(),
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
    YDB_LOG_DEBUG_CTX(*ActorSystem, "OnDirectWriteResponse",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_PrintHostAndNode(host).c_str", PrintHostAndNode(host)},
        {"#_FormatError(response.Error).Quote().c_str", FormatError(response.Error)});

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        MaybeReplyOrNotifyBelated(THostMask::MakeOne(host));
        return;
    }

    FailedWrites.Set(host);
    auto ender = TEndSpanWithError(std::move(span), response.Error);
    MaybeSendReplacementDirectWrite(response.Error);
}

void TWriteRequestExecutor::MaybeSendReplacementDirectWrite(
    const NProto::TError& error)
{
    if (IsReplied) {
        return;
    }

    if (!IsQuorumReachable()) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "It is impossible to reach a quorum",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatError(error).Quote().c_str", FormatError(error)});
        Reply(error);
        return;
    }

    const auto candidates = VChunkConfig.GetDesiredPBuffers()
                                .Include(VChunkConfig.GetSecondaryPBuffers())
                                .Exclude(RequestedDirectWrites)
                                .Exclude(FailedWrites)
                                .Exclude(CompletedWrites)
                                .Exclude(IndirectCoordinator);

    if (candidates.Empty()) {
        YDB_LOG_WARN_CTX(*ActorSystem, "All hand-offs attempts are",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatError(error).Quote().c_str", FormatError(error)});
        return;
    }

    SendDirectWriteRequest(*candidates.First());
}

void TWriteRequestExecutor::MaybeReplyOrNotifyBelated(
    THostMask completedOnCurrentResponse)
{
    if (IsReplied) {
        // A write completed after the reply is belated: its copy is on the
        // PBuffer and has to be erased from there.
        NotifyBelated(completedOnCurrentResponse);
        return;
    }

    if (IsQuorumReached()) {
        Reply(MakeError(S_OK));
    }
}

void TWriteRequestExecutor::Reply(NProto::TError error)
{
    Y_ABORT_IF(IsReplied, "TWriteRequestExecutor::Reply called twice");
    IsReplied = true;

    const auto duration = TInstant::Now() - StartAt;
    if (duration > SlowRequestTime) {
        YDB_LOG_INFO_CTX(*ActorSystem, "[?] Slow request",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatDuration(duration).c_str", FormatDuration(duration)});
    }

    if (HasError(error)) {
        YDB_LOG_ERROR_CTX(*ActorSystem, "[!] Reply error",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatError(error).Quote().c_str", FormatError(error)});

        Y_ABORT_UNLESS(!IsQuorumReached());
    } else {
        YDB_LOG_DEBUG_CTX(*ActorSystem, "Reply OK",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

        Y_ABORT_UNLESS(IsQuorumReached());
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

    YDB_LOG_DEBUG_CTX(*ActorSystem, "NotifyBelated",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
        {"#_completedOnCurrentResponse.Print().c_str", completedOnCurrentResponse.Print()});

    Bundle->NotifyBelated(completedOnCurrentResponse);
}

void TWriteRequestExecutor::ScheduleHedging(TDuration hedgingDelay)
{
    if (!hedgingDelay) {
        return;
    }

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule OnHedgingTimeout",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
        {"#_FormatDuration(hedgingDelay).c_str", FormatDuration(hedgingDelay)});

    DirectBlockGroup->Schedule(
        hedgingDelay,
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

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule OnRequestTimeout()",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_FormatDuration(RequestTimeout).c_str", FormatDuration(RequestTimeout)});

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
    YDB_LOG_DEBUG_CTX(*ActorSystem, "OnHedgingTimeout",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

    if (IsReplied) {
        return;
    }

    switch (WriteMode) {
        case EWriteMode::IndirectWrite: {
            MaybeSendAdditionalDirectWrites();
            break;
        }
        case EWriteMode::DirectWrite: {
            SendDirectWriteRequestsToHandoffs(GetQuorumDeficit());
            break;
        }
    }
}

void TWriteRequestExecutor::OnRequestTimeout()
{
    YDB_LOG_WARN_CTX(*ActorSystem, "Request timeout",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_ExtendedDebugState().c_str", ExtendedDebugState()});

    if (IsReplied) {
        return;
    }

    Reply(MakeError(E_TIMEOUT, "Write request timeout"));
}

bool TWriteRequestExecutor::IsQuorumReached() const
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
    result << "d:" << RequestedDirectWrites.Print();
    result << " i:" << IndirectCoordinator.Print()
           << RequestedIndirectWrites.Print();
    result << " c:" << CompletedWrites.Print();
    result << " f:" << FailedWrites.Print();
    return result;
}

TString TWriteRequestExecutor::PrintHostAndNode(THostIndex host) const
{
    return PrintHostAndNodeId(host, DirectBlockGroup->GetNodeId(host));
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
        std::move(bundle));
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
