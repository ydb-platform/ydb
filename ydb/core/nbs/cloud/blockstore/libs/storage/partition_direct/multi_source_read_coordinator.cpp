#include "multi_source_read_coordinator.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

TMultiSourceReadCoordinator::TMultiSourceReadCoordinator(
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
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Promise(NThreading::NewPromise<TReadRequestExecutor::TResponse>())
{
    const ui32 blockSize = Request->Headers.VolumeConfig->BlockSize;

    SubRequests.reserve(readHint.RangeHints.size());

    for (auto& hint: readHint.RangeHints) {
        // Вычислить смещение в Sglist
        const size_t offsetBlocks = hint.RequestRelativeRange.Start;
        const size_t offsetBytes = offsetBlocks * blockSize;
        const size_t sizeBytes = hint.RequestRelativeRange.Size() * blockSize;

        // Создать подбуфер для этого подзапроса
        auto subRequest = std::make_shared<TReadBlocksLocalRequest>(
            Request->Headers.Clone(hint.VChunkRange));

        // Создать подбуфер Sglist для данного диапазона
        {
            auto guard = Request->Sglist.Acquire();
            if (guard) {
                const TSgList& fullSgList = guard.Get();
                TSgList subSgList =
                    CreateSgListSubRange(fullSgList, offsetBytes, sizeBytes);
                subRequest->Sglist =
                    Request->Sglist.Create(std::move(subSgList));
            }
        }

        // Создать TReadHint с одним hint
        TReadHint singleHint;
        singleHint.RangeHints.push_back(std::move(hint));

        // Создать executor для этого hint
        auto executor = std::make_shared<TReadRequestExecutor>(
            ActorSystem,
            VChunkConfig,
            DirectBlockGroup,
            std::move(singleHint),
            CallContext,
            subRequest,
            NWilson::TTraceId(TraceId));

        SubRequests.push_back(TSubRequest{
            .Executor = std::move(executor),
            .SglistOffset = offsetBytes});
    }
}

void TMultiSourceReadCoordinator::Run()
{
    for (size_t i = 0; i < SubRequests.size(); ++i) {
        auto future = SubRequests[i].Executor->GetFuture();
        future.Subscribe(
            [self = shared_from_this(),
             i](const NThreading::TFuture<TReadRequestExecutor::TResponse>& f)
            {
                Y_UNUSED(f);
                self->OnSubRequestComplete(i);
            });

        SubRequests[i].Executor->Run();
    }
}

void TMultiSourceReadCoordinator::OnSubRequestComplete(size_t index)
{
    const auto& response = SubRequests[index].Executor->GetFuture().GetValue();

    if (HasError(response.Error)) {
        // Первая ошибка - завершить весь запрос
        if (Promise.TrySetValue(response)) {
            // Успешно установили ошибку
            LOG_ERROR(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "TMultiSourceReadCoordinator: SubRequest %zu failed: %s",
                index,
                FormatError(response.Error).c_str());
        }
        return;
    }

    // Проверить, все ли подзапросы завершены
    if (++CompletedCount == SubRequests.size()) {
        // Все успешно
        Promise.SetValue(
            TReadRequestExecutor::TResponse{.Error = MakeError(S_OK)});
    }
}

NThreading::TFuture<TReadRequestExecutor::TResponse>
TMultiSourceReadCoordinator::GetFuture()
{
    return Promise.GetFuture();
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
