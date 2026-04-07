#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/diagnostics/trace_helpers.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/wilson/wilson_span.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

EWriteMode GetWriteModeFromProto(NProto::EWriteMode writeMode)
{
    switch (writeMode) {
        case NProto::EWriteMode::PBufferReplication:
            return EWriteMode::PBufferReplication;
        case NProto::EWriteMode::DirectPBuffersFilling:
            return EWriteMode::DirectPBuffersFilling;
        default:
            break;
    }
    Y_ABORT_UNLESS(false);
}

NProto::EWriteMode GetProtoWriteMode(EWriteMode writeMode)
{
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            return NProto::EWriteMode::PBufferReplication;
        case EWriteMode::DirectPBuffersFilling:
            return NProto::EWriteMode::DirectPBuffersFilling;
    }
}

////////////////////////////////////////////////////////////////////////////////

TWriteRequestExecutor::TWriteRequestExecutor(
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

void TWriteRequestExecutor::Run(
    EWriteMode writeMode,
    TDuration pbufferReplyTimeout)
{
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            SendWriteRequestToManyPBuffers(pbufferReplyTimeout);
            // We don't need to schedule requests to handoff persistent buffers
            // after delay since we will send them in case of error. See
            // OnWriteToManyPBuffersResponse.
            return;
        case EWriteMode::DirectPBuffersFilling:
            SendWriteRequest(ELocation::PBuffer0);
            SendWriteRequest(ELocation::PBuffer1);
            SendWriteRequest(ELocation::PBuffer2);

            if (HedgingDelay) {
                DirectBlockGroup->Schedule(
                    HedgingDelay,
                    [weakSelf = weak_from_this()]()
                    {
                        if (auto self = weakSelf.lock()) {
                            self->SendWriteRequestsToHandoffPBuffers();
                        }
                    });
            }
            return;
    }
}

NThreading::TFuture<TWriteRequestExecutor::TResponse>
TWriteRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TWriteRequestExecutor::SendWriteRequestToManyPBuffers(
    TDuration pbufferReplyTimeout)
{
    std::vector<ELocation> locations = {
        ELocation::PBuffer0,
        ELocation::PBuffer1,
        ELocation::PBuffer2};

    std::vector<ui8> hostsIndexes;
    hostsIndexes.reserve(3);
    for (auto location: locations) {
        hostsIndexes.push_back(VChunkConfig.GetHostIndex(location));
        RequestedWrites.Set(location);
    }

    auto future = DirectBlockGroup->WriteBlocksToManyPBuffers(
        VChunkConfig.VChunkIndex,
        std::move(hostsIndexes),
        Lsn,
        VChunkRange,
        pbufferReplyTimeout,
        Request->Sglist,
        NWilson::TTraceId(TraceId));

    future.Subscribe(
        [self = shared_from_this()](
            const NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>& f)
        { self->OnWriteToManyPBuffersResponse(f.GetValue()); });
}

void TWriteRequestExecutor::OnWriteToManyPBuffersResponse(
    const TDBGWriteBlocksToManyPBuffersResponse& response)
{
    if (HasError(response.OverallError)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse fatal error: %s",
            FormatError(response.OverallError).c_str());
        // The error will be set and replied below.
    } else {
        for (const auto& pbufferResponse: response.Responses) {
            auto location =
                VChunkConfig.GetPBufferLocation(pbufferResponse.HostId);
            if (!HasError(pbufferResponse.Error)) {
                CompletedWrites.Set(location);
            } else {
                LOG_WARN(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "OnWriteToManyPBuffersResponse error on location %d: %s",
                    location,
                    FormatError(pbufferResponse.Error).c_str());
            }
        }
    }

    if (CompletedWrites.Count() >= QuorumDirectBlockGroupHostCount) {
        Reply(MakeError(S_OK));
        return;
    }

    std::vector<ELocation> handoffLocations(
        {ELocation::HOPBuffer0, ELocation::HOPBuffer1});
    if (CompletedWrites.Count() + handoffLocations.size() <
        QuorumDirectBlockGroupHostCount)
    {
        auto resultError =
            MakeError(E_FAIL, "Hand-offs retries are not available");
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "OnWriteToManyPBuffersResponse: %s",
            FormatError(resultError).c_str());

        Reply(resultError);
        return;
    }

    // Sending request to handoff in case of 1-2 errors
    for (size_t i = 0;
         i < QuorumDirectBlockGroupHostCount - CompletedWrites.Count();
         ++i)
    {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "trying to send fallback writeRequest to %d handoff",
            i);
        SendWriteRequest(handoffLocations[i]);
    }
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

void TWriteRequestExecutor::SendWriteRequestsToHandoffPBuffers()
{
    if (CompletedWrites.Count() <= QuorumDirectBlockGroupHostCount - 1) {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteRequestExecutor. Send write request to HOPBuffer0 since we "
            "have %lu completed writes",
            CompletedWrites.Count());

        SendWriteRequest(ELocation::HOPBuffer0);
    }

    if (CompletedWrites.Count() <= QuorumDirectBlockGroupHostCount - 2) {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TWriteRequestExecutor. Send write request to HOPBuffer1 since we "
            "have %lu completed writes",
            CompletedWrites.Count());

        SendWriteRequest(ELocation::HOPBuffer1);
    }
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
