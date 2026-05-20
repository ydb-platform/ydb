#include "read_request_multiple_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadMultipleLocationRequestExecutor::TReadMultipleLocationRequestExecutor(
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
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
{
    Y_ASSERT(Request->Headers.VolumeConfig);
    Y_ASSERT(Request->Headers.VolumeConfig->BlockSize != 0);

    const size_t blockSize = Request->Headers.VolumeConfig->BlockSize;

    SubRequestExecutors.reserve(readHint.RangeHints.size());
    for (size_t i = 0; i < readHint.RangeHints.size(); ++i) {
        auto& hint = readHint.RangeHints[i];
        // Compute offset for Sglist
        const size_t offsetBlocks = hint.RequestRelativeRange.Start;
        const size_t offsetBytes = offsetBlocks * blockSize;
        const size_t sizeBytes = hint.RequestRelativeRange.Size() * blockSize;

        auto subRequest = std::make_shared<TReadBlocksLocalRequest>(
            Request->Headers.Clone(hint.VChunkRange));

        // Create subbuffer Sglist for current range

        if (auto guard = Request->Sglist.Acquire()) {
            const TSgList& fullSgList = guard.Get();
            TSgList subSgList =
                CreateSgListSubRange(fullSgList, offsetBytes, sizeBytes);
            subRequest->Sglist =
                Request->Sglist.CreateDepender(std::move(subSgList));
        } else {
            Reply(MakeError(E_CANCELLED, "Failed to acquire sglist guard"), i);
            return;
        }

        TReadHint singleHint;
        singleHint.RangeHints.push_back(std::move(hint));
        auto executor = std::make_shared<TReadSingleLocationRequestExecutor>(
            ActorSystem,
            LogTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(singleHint),
            CallContext,
            subRequest,
            NWilson::TTraceId(TraceId));

        SubRequestExecutors.push_back(std::move(executor));
    }
}

TReadMultipleLocationRequestExecutor::~TReadMultipleLocationRequestExecutor()
{
    if (!Promise.IsReady()) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Reply has not been sent.",
            LogTitle.GetWithTime().c_str());

        Y_ABORT_UNLESS(false);
    }
}

void TReadMultipleLocationRequestExecutor::Run()
{
    for (size_t i = 0; i < SubRequestExecutors.size(); ++i) {
        auto future = SubRequestExecutors[i]->GetFuture();
        future.Subscribe([self = shared_from_this(),
                          i](const NThreading::TFuture<TResponse>& f)
                         { self->OnSubRequestComplete(f.GetValue(), i); });

        SubRequestExecutors[i]->Run();
    }
}

NThreading::TFuture<IReadRequestExecutor::TResponse>
TReadMultipleLocationRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TReadMultipleLocationRequestExecutor::OnSubRequestComplete(
    const TResponse& response,
    size_t index)
{
    ++CompletedCount;

    if (HasError(response.Error)) {
        // Complete full request with an error in case of subrequest's error
        Reply(response.Error, index);
        return;
    }

    if (CompletedCount == SubRequestExecutors.size()) {
        Reply(MakeError(S_OK), index);
    }
}

void TReadMultipleLocationRequestExecutor::Reply(
    NProto::TError error,
    size_t index)
{
    if (HasError(error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s Request: %zu, Error: %s",
            LogTitle.GetWithTime().c_str(),
            index,
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
