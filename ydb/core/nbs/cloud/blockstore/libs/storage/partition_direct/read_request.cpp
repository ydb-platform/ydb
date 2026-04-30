#include "read_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

////////////////////////////////////////////////////////////////////////////////

TReadHint ArmLocks(TReadHint readHint)
{
    for (auto& hint: readHint.RangeHints) {
        hint.Lock.Arm();
    }
    return readHint;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadRequestExecutor::TReadRequestExecutor(
    NActors::TActorSystem* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , ReadHint(ArmLocks(std::move(readHint)))
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
            Request->Headers.Range.Print().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TReadRequestExecutor::Run()
{
    Y_ABORT_UNLESS(ReadHint.RangeHints.size() == 1);

    const auto& hint = ReadHint.RangeHints[0];

    std::optional<THostIndex> host;
    size_t skipped = 0;
    for (auto h: hint.HostMask) {
        if (skipped++ < TryNumber) {
            continue;
        }
        host = h;
        break;
    }
    if (!host) {
        TString error = TStringBuilder()
                        << "Can't read. Mask:" << hint.HostMask.Print()
                        << " try:" << TryNumber;
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadRequestExecutor %s %s",
            hint.VChunkRange.Print().c_str(),
            error.c_str());

        Reply(MakeError(E_REJECTED, error));
        return;
    }

    const bool fromDDisk = hint.FromDDisk;

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TReadRequestExecutor. Reading from host %u (%s)",
        static_cast<unsigned>(*host),
        fromDDisk ? "DDisk" : "PBuffer");

    auto onReadResponse = [self = shared_from_this()]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
    {
        self->OnReadResponse(f.GetValue());
    };

    auto future = fromDDisk ? DirectBlockGroup->ReadBlocksFromDDisk(
                                  VChunkConfig.VChunkIndex,
                                  *host,
                                  hint.VChunkRange,
                                  Request->Sglist,
                                  TraceId)
                            : DirectBlockGroup->ReadBlocksFromPBuffer(
                                  VChunkConfig.VChunkIndex,
                                  *host,
                                  hint.Lsn,
                                  hint.VChunkRange,
                                  Request->Sglist,
                                  TraceId);
    future.Subscribe(std::move(onReadResponse));
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

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TReadRequestExecutor: OnReadResponse failed %d trying. Error: %s",
        TryNumber,
        FormatError(response.Error).c_str());

    ++TryNumber;
    Run();
}

void TReadRequestExecutor::Reply(NProto::TError error)
{
    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
