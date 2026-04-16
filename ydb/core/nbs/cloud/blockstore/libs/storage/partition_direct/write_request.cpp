#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>

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
    TDuration hedgingDelay)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkRange(vChunkRange)
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Lsn(lsn)
    , HedgingDelay(hedgingDelay)
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

void TBaseWriteRequestExecutor::SendWriteRequest(ELocation location)
{
    auto span = std::make_shared<NWilson::TSpan>(NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        TraceId.Clone(),
        "TBaseWriteRequestExecutor",
        NWilson::EFlags::AUTO_END,
        ActorSystem));
    span->Attribute("Location", ToString(location));

    RequestedWrites.Set(location);

    auto future = DirectBlockGroup->WriteBlocksToPBuffer(
        VChunkConfig.VChunkIndex,
        VChunkConfig.GetHostIndex(location),
        Lsn,
        VChunkRange,
        Request->Sglist,
        span->GetTraceId());

    future.Subscribe(
        [self = shared_from_this(), location, span = std::move(span)]       //
        (const NThreading::TFuture<TDBGWriteBlocksResponse>& f) mutable {   //
            self->OnWriteResponse(location, f.GetValue(), std::move(span));
        });
}

void TBaseWriteRequestExecutor::OnWriteResponse(
    ELocation location,
    const TDBGWriteBlocksResponse& response,
    std::shared_ptr<NWilson::TSpan> span)
{
    if (!HasError(response.Error)) {
        CompletedWrites.Set(location);
        if (CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount) {
            Reply(MakeError(S_OK));
        }
        return;
    }

    if (!RequestedWrites.Get(ELocation::HOPBuffer0)) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TBaseWriteRequestExecutor. Try first hand-off. %s",
            FormatError(response.Error).c_str());

        SendWriteRequest(ELocation::HOPBuffer0);
    } else if (!RequestedWrites.Get(ELocation::HOPBuffer1)) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TBaseWriteRequestExecutor. Try second hand-off. %s",
            FormatError(response.Error).c_str());

        SendWriteRequest(ELocation::HOPBuffer1);
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

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
