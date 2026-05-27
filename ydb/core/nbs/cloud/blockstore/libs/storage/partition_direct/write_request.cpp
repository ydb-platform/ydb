#include "write_request.h"

#include "direct_block_group.h"
#include "write_with_direct_replication_request.h"
#include "write_with_pb_replication_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/model/oracle.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TBaseWriteRequestExecutor::TBaseWriteRequestExecutor(
    NActors::TActorSystem* actorSystem,
    TChildLogTitle logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TBlockRange64 vChunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    ui64 lsn,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , LogTitle(std::move(logTitle))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkRange(vChunkRange)
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Lsn(lsn)
    , HedgingDelay(DirectBlockGroup->GetOracle()->GetWriteHedgingDelay())
    , RequestTimeout(DirectBlockGroup->GetOracle()->GetWriteRequestTimeout())
{}

TBaseWriteRequestExecutor::~TBaseWriteRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply not sent.",
            LogTitle.GetWithTime().c_str());

        Y_ABORT_UNLESS(false);
    }
}

NThreading::TFuture<TBaseWriteRequestExecutor::TResponse>
TBaseWriteRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TBaseWriteRequestExecutor::Reply(NProto::TError error)
{
    if (HasError(error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply error %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply OK.",
            LogTitle.GetWithTime().c_str());
    }

    Request->Sglist.Close();

    Promise.TrySetValue(TResponse{
        .Error = std::move(error),
        .Lsn = Lsn,
        .RequestedWrites = RequestedWrites,
        .CompletedWrites = CompletedWrites});
}

void TBaseWriteRequestExecutor::SendWriteRequest(THostIndex host)
{
    if (Promise.IsReady()) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s SendWriteRequest. HostIndex: %u",
        LogTitle.GetWithTime().c_str(),
        static_cast<ui32>(host));

    auto span =
        DirectBlockGroup->CreateChildSpan(TraceId, "TBaseWriteRequestExecutor");
    if (span) {
        span->Attribute("HostIndex", ToString(host));
    }

    RequestedWrites.Set(host);

    auto future = DirectBlockGroup->WriteBlocksToPBuffer(
        VChunkConfig.VChunkIndex,
        host,
        Lsn,
        VChunkRange,
        Request->Sglist,
        span ? span->GetTraceId() : NWilson::TTraceId());

    future.Subscribe(
        [self = shared_from_this(), host, span = std::move(span)]           //
        (const NThreading::TFuture<TDBGWriteBlocksResponse>& f) mutable {   //
            self->OnWriteResponse(host, f.GetValue(), std::move(span));
        });
}

void TBaseWriteRequestExecutor::OnWriteResponse(
    THostIndex host,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    if (Promise.IsReady()) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s OnWriteResponse. HostIndex: %u, Error: %s",
        LogTitle.GetWithTime().c_str(),
        static_cast<ui32>(host),
        FormatError(response.Error).c_str());

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        if (ShouldReplyOk()) {
            Reply(MakeError(S_OK));
        }
        return;
    }

    const auto candidates =
        VChunkConfig.PBufferHosts.GetHandOff().Exclude(RequestedWrites);
    if (auto next = candidates.First()) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Try hand-off. %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response.Error).c_str());

        SendWriteRequest(*next);
    } else {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s All hand-offs attempts are over. %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(response.Error).c_str());

        Reply(response.Error);

        auto ender = TEndSpanWithError(std::move(span), response.Error);
    }
}

void TBaseWriteRequestExecutor::ScheduleRequestTimeoutCallback()
{
    if (!RequestTimeout) {
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s ScheduleRequestTimeoutCallback",
        LogTitle.GetWithTime().c_str());

    DirectBlockGroup->Schedule(
        RequestTimeout,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->RequestTimeoutCallback();
            }
        });
}

void TBaseWriteRequestExecutor::RequestTimeoutCallback()
{
    LOG_WARN(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Request timeout.",
        LogTitle.GetWithTime().c_str());

    Reply(MakeError(E_TIMEOUT, "Write request timeout"));
}

bool TBaseWriteRequestExecutor::ShouldReplyOk() const
{
    return CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount;
}

TVector<THostIndex> TBaseWriteRequestExecutor::GetAvailableHandOffHosts() const
{
    return VChunkConfig.PBufferHosts.GetHandOff()
        .Exclude(RequestedWrites)
        .Hosts();
}

////////////////////////////////////////////////////////////////////////////////

TBaseWriteRequestExecutorPtr CreateWriteRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TBlockRange64 vChunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    ui64 lsn,
    NWilson::TTraceId traceId)
{
    EWriteMode writeMode = directBlockGroup->GetOracle()->GetWriteMode();
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            return std::make_shared<TWriteWithPbReplicationRequestExecutor>(
                actorSystem,
                logTitle.GetChildWithTags(
                    GetCycleCount(),
                    {{"t", "p-write"}, {"r", vChunkRange.Print()}}),
                vChunkConfig,
                std::move(directBlockGroup),
                vChunkRange,
                std::move(callContext),
                std::move(request),
                lsn,
                std::move(traceId));
            break;
        case EWriteMode::DirectPBuffersFilling:
            return std::make_shared<TWriteWithDirectReplicationRequestExecutor>(
                actorSystem,
                logTitle.GetChildWithTags(
                    GetCycleCount(),
                    {{"t", "d-write"}, {"r", vChunkRange.Print()}}),
                vChunkConfig,
                std::move(directBlockGroup),
                vChunkRange,
                std::move(callContext),
                std::move(request),
                lsn,
                std::move(traceId));
            break;
    }
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
