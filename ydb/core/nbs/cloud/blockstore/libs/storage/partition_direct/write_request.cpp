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
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TBlockRange64 vChunkRange,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    ui64 lsn,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
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
            "TBaseWriteRequestExecutor. Reply not sent %s %s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

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

    if (!HasError(response.Error)) {
        CompletedWrites.Set(host);
        if (CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount) {
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
            "TBaseWriteRequestExecutor. Try hand-off. %s",
            FormatError(response.Error).c_str());

        SendWriteRequest(*next);
    } else {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TBaseWriteRequestExecutor. All hand-offs attempts are over. %s",
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
        "TBaseWriteRequestExecutor. Write request timeout. %s %s",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str());

    Reply(MakeError(E_TIMEOUT, "Write request timeout"));
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
