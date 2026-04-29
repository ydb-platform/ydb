#include "write_request.h"

#include "direct_block_group.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>

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
    NWilson::TTraceId traceId,
    TDuration hedgingDelay,
    TDuration timeout)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkRange(vChunkRange)
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Lsn(lsn)
    , HedgingDelay(hedgingDelay)
    , RequestTimeout(timeout)
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
    TVector<THostIndex> hosts;
    const auto candidates =
        VChunkConfig.PBufferHosts.GetHandOff().Exclude(RequestedWrites);
    for (auto h: candidates) {
        hosts.push_back(h);
    }
    return hosts;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
