#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

EWriteMode GetWriteModeFromProto(
    NProto::TStorageServiceConfig::TWriteMode writeMode)
{
    switch (writeMode) {
        case NProto::TStorageServiceConfig::PBufferReplication:
            return EWriteMode::PBufferReplication;
        case NProto::TStorageServiceConfig::DirectPBuffersFilling:
            return EWriteMode::DirectPBuffersFilling;
        default:
            break;
    }
    Y_ABORT_UNLESS(false);
}

NProto::TStorageServiceConfig::TWriteMode GetProtoWriteMode(
    EWriteMode writeMode)
{
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            return NProto::TStorageServiceConfig::PBufferReplication;
        case EWriteMode::DirectPBuffersFilling:
            return NProto::TStorageServiceConfig::DirectPBuffersFilling;
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
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , VChunkRange(vChunkRange)
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Lsn(Request->Lsn)
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
    ui32 pbufferReplyTimeoutMicroseconds)
{
    switch (writeMode) {
        case EWriteMode::PBufferReplication:
            SendWriteRequestToManyPBuffers(pbufferReplyTimeoutMicroseconds);
            return;
        case EWriteMode::DirectPBuffersFilling:
            SendWriteRequest(ELocation::PBuffer0);
            SendWriteRequest(ELocation::PBuffer1);
            SendWriteRequest(ELocation::PBuffer2);
            return;
    }
}

NThreading::TFuture<TWriteRequestExecutor::TResponse>
TWriteRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TWriteRequestExecutor::SendWriteRequestToManyPBuffers(
    ui32 pbufferReplyTimeoutMicroseconds)
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
        pbufferReplyTimeoutMicroseconds,
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
    for (const auto& pbufferResponse: response.Responses) {
        auto location = VChunkConfig.GetPBufferLocation(pbufferResponse.HostId);
        if (!HasError(pbufferResponse.Error)) {
            CompletedWrites.Set(location);
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
            "TWriteRequestExecutor: %s",
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
    RequestedWrites.Set(location);

    auto future = DirectBlockGroup->WriteBlocksToPBuffer(
        VChunkConfig.VChunkIndex,
        VChunkConfig.GetHostIndex(location),
        Lsn,
        VChunkRange,
        Request->Sglist,
        NWilson::TTraceId(TraceId));

    future.Subscribe([self = shared_from_this(), location]   //
                     (const NThreading::TFuture<TDBGWriteBlocksResponse>& f)
                     { self->OnWriteResponse(location, f.GetValue()); });
}

void TWriteRequestExecutor::OnWriteResponse(
    ELocation location,
    const TDBGWriteBlocksResponse& response)
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
