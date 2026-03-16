#include "read_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadRequestExecutor::TReadRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TVector<TReadHint> hints,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , Hints(std::move(hints))
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
{}

TReadRequestExecutor::~TReadRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadRequestExecutor. Reply not sent %s %s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Range.Print().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TReadRequestExecutor::Run()
{
    Y_ABORT_UNLESS(Hints.size() == 1);

    const auto& hint = Hints[0];

    std::optional<ELocation> location =
        hint.LocationMask.GetLocation(TryNumber);
    if (!location) {
        TString error = TStringBuilder()
                        << "Can't read. Mask:" << hint.LocationMask.Print()
                        << " try:" << TryNumber;
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadRequestExecutor %s %s",
            hint.Range.Print().c_str(),
            error.c_str());

        Reply(MakeError(E_REJECTED, error));
        return;
    }

    auto future = IsDDisk(*location) ? DirectBlockGroup->ReadBlocksFromDDisk(
                                           VChunkConfig.VChunkIndex,
                                           VChunkConfig.GetHostIndex(*location),
                                           hint.Range,
                                           Request->Sglist,
                                           NWilson::TTraceId(TraceId))
                                     : DirectBlockGroup->ReadBlocksFromPBuffer(
                                           VChunkConfig.VChunkIndex,
                                           VChunkConfig.GetHostIndex(*location),
                                           hint.Lsn,
                                           hint.Range,
                                           Request->Sglist,
                                           NWilson::TTraceId(TraceId));

    future.Subscribe([self = shared_from_this()]   //
                     (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
                     { self->OnReadResponse(f.GetValue()); });
}

NThreading::TFuture<TReadRequestExecutor::TResponse>
TReadRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TReadRequestExecutor::OnReadResponse(
    const TDBGReadBlocksResponse& response)
{
    if (!HasError(response.Error)) {
        Reply(response.Error);
        return;
    }

    ++TryNumber;
    Run();
}

void TReadRequestExecutor::Reply(NProto::TError error)
{
    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
