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
    TChildLogTitle logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , LogTitle(std::move(logTitle))
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
            "%s Reply not sent.",
            LogTitle.GetWithTime().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TReadSingleLocationRequestExecutor::Run()
{
    Y_ABORT_UNLESS(ReadHint.RangeHints.size() == 1);

    const auto& hint = ReadHint.RangeHints[0];

    auto host = hint.HostMask.Nth(TryNumber);
    if (!host) {
        Reply(MakeError(
            E_REJECTED,
            TStringBuilder() << "Can't read. Mask:" << hint.HostMask.Print()
                             << " try:" << TryNumber));
        return;
    }

    const bool fromDDisk = hint.Lsn == 0;

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Will read from %s of host %zu",
        LogTitle.GetWithTime().c_str(),
        fromDDisk ? "DDisk" : "PBuffer",
        static_cast<size_t>(*host));

    auto onReadResponse = [self = shared_from_this()]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
    {
        self->OnReadResponse(f.GetValue());
    };

    auto future = fromDDisk ? DirectBlockGroup->ReadBlocksFromDDisk(
                                  VChunkConfig.GetVChunkIndex(),
                                  *host,
                                  hint.VChunkRange,
                                  Request->Sglist,
                                  TraceId)
                            : DirectBlockGroup->ReadBlocksFromPBuffer(
                                  VChunkConfig.GetVChunkIndex(),
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
    const bool retriesLimitReached = TryNumber == 3 - 1;   // can try 3 times
    if (!HasError(response.Error) || retriesLimitReached) {
        Reply(response.Error);
        return;
    }

    ++TryNumber;

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Retrying: %zu, Error: %s",
        LogTitle.GetWithTime().c_str(),
        TryNumber,
        FormatError(response.Error).c_str());

    Run();
}

void TReadSingleLocationRequestExecutor::Reply(NProto::TError error)
{
    if (HasError(error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Error: %s",
            LogTitle.GetWithTime().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OK",
            LogTitle.GetWithTime().c_str());
    }

    Request->Sglist.Close();

    Promise.TrySetValue(TResponse{.Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
