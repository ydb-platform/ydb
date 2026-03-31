#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TWriteRequestExecutor::TWriteRequestExecutor(
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
{}

TWriteRequestExecutor::~TWriteRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteRequestExecutor. Reply not sent %s %s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TWriteRequestExecutor::Run()
{
    SendWriteRequest(ELocation::PBuffer0);
    SendWriteRequest(ELocation::PBuffer1);
    SendWriteRequest(ELocation::PBuffer2);
}

NThreading::TFuture<TWriteRequestExecutor::TResponse>
TWriteRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TWriteRequestExecutor::SendWriteRequest(ELocation location)
{
    auto span = std::make_shared<NWilson::TSpan>(NWilson::TSpan(
        NKikimr::TWilsonNbs::NbsBasic,
        TraceId.Clone(),
        "TWriteRequestExecutor",
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

void TWriteRequestExecutor::OnWriteResponse(
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
            "TWriteRequestExecutor. Try first hand-off. %s",
            FormatError(response.Error).c_str());

        SendWriteRequest(ELocation::HOPBuffer0);
    } else if (!RequestedWrites.Get(ELocation::HOPBuffer1)) {
        LOG_WARN(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteRequestExecutor. Try second hand-off. %s",
            FormatError(response.Error).c_str());

        SendWriteRequest(ELocation::HOPBuffer1);
    } else {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteRequestExecutor. All hand-offs attempts are over. %s",
            FormatError(response.Error).c_str());

        Reply(response.Error);

        auto ender = TEndSpanWithError(std::move(span), response.Error);
    }
}

void TWriteRequestExecutor::Reply(NProto::TError error)
{
    Promise.TrySetValue(TResponse{
        .Error = std::move(error),
        .Lsn = Lsn,
        .RequestedWrites = RequestedWrites,
        .CompletedWrites = CompletedWrites});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
