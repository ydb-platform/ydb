#include "read_request_single_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
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

TReadSingleLocationRequestExecutor::TReadSingleLocationRequestExecutor(
    NActors::TActorSystem const* actorSystem,
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

TReadSingleLocationRequestExecutor::~TReadSingleLocationRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadSingleLocationRequestExecutor. Reply not sent %s %s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TReadSingleLocationRequestExecutor::Run()
{
    Y_ABORT_UNLESS(ReadHint.RangeHints.size() == 1);

    const auto& hint = ReadHint.RangeHints[0];

    auto host = hint.HostMask.Nth(TryNumber);
    if (!host) {
        TString error = TStringBuilder()
                        << "Can't read. Mask:" << hint.HostMask.Print()
                        << " try:" << TryNumber;
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadSingleLocationRequestExecutor failed to find location %s %s "
            "%s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str(),
            error.c_str());

        Reply(MakeError(E_REJECTED, error));
        return;
    }

    const bool fromDDisk = hint.Lsn == 0;

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TReadSingleLocationRequestExecutor %s %s. Reading from host %u (%s)",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str(),
        static_cast<ui32>(*host),
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

NThreading::TFuture<IReadRequestExecutor::TResponse>
TReadSingleLocationRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TReadSingleLocationRequestExecutor::OnReadResponse(
    const TDBGReadBlocksResponse& response)
{
    bool retriesLimitReached = TryNumber == 3;
    if (!HasError(response.Error) || retriesLimitReached) {
        if (retriesLimitReached) {
            LOG_ERROR(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "TReadSingleLocationRequestExecutor: read failed %s %s with "
                "error '%s'",
                Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
                Request->Headers.Range.Print().c_str(),
                FormatError(response.Error).c_str());
        }

        Reply(response.Error);
        return;
    }

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TReadSingleLocationRequestExecutor: OnReadResponse %s %s failed %d "
        "trying with error '%s'",
        Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
        Request->Headers.Range.Print().c_str(),
        TryNumber,
        FormatError(response.Error).c_str());

    ++TryNumber;
    Run();
}

void TReadSingleLocationRequestExecutor::Reply(NProto::TError error)
{
    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
