#include "read_request_multiple_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TReadMultipleLocationRequestExecutor::TReadMultipleLocationRequestExecutor(
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
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Promise(NThreading::NewPromise<TResponse>())
{
    SubRequestExecutors.reserve(readHint.RangeHints.size());

    Y_ASSERT(Request->Headers.VolumeConfig);
    Y_ASSERT(Request->Headers.VolumeConfig->BlockSize != 0);
    size_t blockSize = Request->Headers.VolumeConfig->BlockSize;

    for (auto& hint: readHint.RangeHints) {
        // Compute offset for Sglist
        const size_t offsetBlocks = hint.RequestRelativeRange.Start;
        const size_t offsetBytes = offsetBlocks * blockSize;
        const size_t sizeBytes = hint.RequestRelativeRange.Size() * blockSize;

        auto subRequest = std::make_shared<TReadBlocksLocalRequest>(
            Request->Headers.Clone(hint.VChunkRange));

        // Create subbuffer Sglist for current range
        {
            auto guard = Request->Sglist.Acquire();
            if (guard) {
                const TSgList& fullSgList = guard.Get();
                TSgList subSgList =
                    CreateSgListSubRange(fullSgList, offsetBytes, sizeBytes);
                subRequest->Sglist =
                    Request->Sglist.CreateDepender(std::move(subSgList));
            } else {
                auto error =
                    MakeError(E_CANCELLED, "Failed to acquire sglist guard");
                LOG_ERROR(
                    *ActorSystem,
                    NKikimrServices::NBS_PARTITION,
                    "TReadRequestExecutor: SubRequest %s %s failed: %s",
                    Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
                    Request->Headers.Range.Print().c_str(),
                    FormatError(error).c_str());

                Promise.TrySetValue(TResponse{.Error = std::move(error)});
                return;
            }
        }

        TReadHint singleHint;
        singleHint.RangeHints.push_back(std::move(hint));
        auto executor = std::make_shared<TReadSingleLocationRequestExecutor>(
            ActorSystem,
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
            "TReadRequestExecutor. Reply has not been sent %s %s",
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str());

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

void TReadMultipleLocationRequestExecutor::OnSubRequestComplete(
    const TResponse& response,
    size_t index)
{
    if (HasError(response.Error)) {
        // Complete full request with an error in case of subrequest's error
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadRequestExecutor: SubRequest %zu %s %s failed: %s",
            index,
            Request->Headers.VolumeConfig->DiskId.Quote().c_str(),
            Request->Headers.Range.Print().c_str(),
            FormatError(response.Error).c_str());

        ++CompletedCount;
        Promise.TrySetValue(response);
        return;
    }

    if (++CompletedCount == SubRequestExecutors.size()) {
        Promise.TrySetValue(TResponse{.Error = MakeError(S_OK)});
    }
}

NThreading::TFuture<TReadMultipleLocationRequestExecutor::TResponse>
TReadMultipleLocationRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
