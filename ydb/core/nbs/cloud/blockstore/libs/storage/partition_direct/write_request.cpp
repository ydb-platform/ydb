#include "write_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

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
    , WriteMode(Request->WriteMode)
    , PBufferReplyTimeoutMicroseconds(Request->PBufferReplyTimeoutMicroseconds)
{
    constexpr ui32 DefaultPBufferReplyTimeoutMicroseconds = 50000;
    if (!PBufferReplyTimeoutMicroseconds) {
        PBufferReplyTimeoutMicroseconds =
            DefaultPBufferReplyTimeoutMicroseconds;
    }
}

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
    if (WriteMode == NProto::TStorageServiceConfig::PBufferReplication) {
        SendWriteRequestToManyPBuffers();
        return;
    }
    if (WriteMode == NProto::TStorageServiceConfig::DirectPBuffersFilling) {
        SendWriteRequest(ELocation::PBuffer0);
        SendWriteRequest(ELocation::PBuffer1);
        SendWriteRequest(ELocation::PBuffer2);
        return;
    }

    auto resultError = MakeError(
        E_FAIL,
        "Unsupported write mode: " + std::to_string(WriteMode));
    LOG_ERROR(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TWriteRequestExecutor: %s",
        FormatError(resultError).c_str());

    Reply(resultError);
}

NThreading::TFuture<TWriteRequestExecutor::TResponse>
TWriteRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TWriteRequestExecutor::SendWriteRequestToManyPBuffers()
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
        PBufferReplyTimeoutMicroseconds,
        Request->Sglist,
        NWilson::TTraceId(TraceId),
        DDiskIdToHostIndex);

    future.Subscribe(
        [self = shared_from_this()](
            const NThreading::TFuture<TDBGWriteBlocksToManyPBuffersResponse>& f)
        { self->OnWriteToManyPBuffersResponse(f.GetValue()); });
}

void TWriteRequestExecutor::OnWriteToManyPBuffersResponse(
    const TDBGWriteBlocksToManyPBuffersResponse& response)
{
    for (const auto& pbufferResponse: response.Responses) {
        const auto& pbufferDiskId = pbufferResponse.PersistentBufferId;
        auto hostId = DDiskIdToHostIndex.find(pbufferDiskId);
        if (hostId == DDiskIdToHostIndex.end()) {
            LOG_ERROR(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "TWriteRequestExecutor. Unexpected pbufferDiskId.");

            continue;
        }
        auto location = VChunkConfig.GetPBufferLocation(hostId->second);
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
