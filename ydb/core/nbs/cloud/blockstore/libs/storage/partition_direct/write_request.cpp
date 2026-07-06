#include "write_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NBS_PARTITION

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
           {"lsn", ToString(bundle->GetLsn())},
           {"r", bundle->GetRange().Print()},
           {"rv", bundle->GetVChunkRange().Print()}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Bundle(std::move(bundle))
    , HedgingDelay(DirectBlockGroup->GetOracle()->GetWriteHedgingDelay())
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
    Bundle->GetSpan().Event("Run");

    const auto hosts = VChunkConfig.GetDesiredPBuffers();
    if (hosts.Count() < QuorumDirectBlockGroupHostCount) {
        Reply(MakeError(E_REJECTED, "Not enough PBuffer hosts"));
        return;
    }

    ScheduleRequestTimeout();
    ScheduleHedging();

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
    THostMask completedWritesOfCurrentResponse;
    for (const auto& pbufferResponse: response.Responses) {
        const auto host = pbufferResponse.HostIndex;

        if (!HasError(pbufferResponse.Error)) {
            YDB_LOG_DEBUG_CTX(*ActorSystem, "OnIndirectWriteResponse OK",
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"#_PrintHostIndex(host).c_str", PrintHostIndex(host)});

            completedWritesOfCurrentResponse.Set(host);
        } else {
            YDB_LOG_WARN_CTX(*ActorSystem, "OnIndirectWriteResponse",
                {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
                {"#_PrintHostIndex(host).c_str", PrintHostIndex(host)},
                {"#_FormatError(pbufferResponse.Error).c_str", FormatError(pbufferResponse.Error)});

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

    YDB_LOG_INFO_CTX(*ActorSystem, "SendAdditionalDirectWrites",
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

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Send DirectWriteRequest",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_PrintHostIndex(host).c_str", PrintHostIndex(host)},
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
    YDB_LOG_DEBUG_CTX(*ActorSystem, "OnDirectWriteResponse",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_PrintHostIndex(host).c_str", PrintHostIndex(host)},
        {"#_FormatError(response.Error).c_str", FormatError(response.Error)});

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
        YDB_LOG_ERROR_CTX(*ActorSystem, "It is impossible to reach a quorum",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatError(response.Error).c_str", FormatError(response.Error)});
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
        YDB_LOG_WARN_CTX(*ActorSystem, "All hand-offs attempts are",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatError(response.Error).c_str", FormatError(response.Error)});
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
        YDB_LOG_ERROR_CTX(*ActorSystem, "Reply error",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
            {"#_ExtendedDebugState().c_str", ExtendedDebugState()},
            {"#_FormatError(error).c_str", FormatError(error)});
    } else {
        YDB_LOG_DEBUG_CTX(*ActorSystem, "Reply OK",
            {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});
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

void TWriteRequestExecutor::ScheduleHedging()
{
    if (!HedgingDelay) {
        return;
    }

    YDB_LOG_DEBUG_CTX(*ActorSystem, "Schedule OnHedgingTimeout()",
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()},
        {"#_FormatDuration(HedgingDelay).c_str", FormatDuration(HedgingDelay)});

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

    switch (WriteMode) {
        case EWriteMode::IndirectWrite: {
            SendAdditionalDirectWrites();
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
        {"#_LogTitle.GetWithTime().c_str", LogTitle.GetWithTime()});

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
