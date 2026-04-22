#include "read_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

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

using TReadResponse = TReadRequestExecutor::TResponse;

class TReadSingleLocationRequestExecutor
    : public std::enable_shared_from_this<TReadSingleLocationRequestExecutor>
{
public:
    TReadSingleLocationRequestExecutor(
        NActors::TActorSystem* actorSystem,
        const TVChunkConfig& vChunkConfig,
        IDirectBlockGroupPtr directBlockGroup,
        TReadHint readHint,
        TCallContextPtr callContext,
        std::shared_ptr<TReadBlocksLocalRequest> request,
        NWilson::TTraceId traceId);

    ~TReadSingleLocationRequestExecutor();

    void Run();

    NThreading::TFuture<TReadResponse> GetFuture() const;

private:
    void OnReadResponse(const TDBGReadBlocksResponse& response);
    void Reply(NProto::TError error);

    NActors::TActorSystem const* ActorSystem;
    const TVChunkConfig VChunkConfig;
    const IDirectBlockGroupPtr DirectBlockGroup;
    const TReadHint ReadHint;
    const TCallContextPtr CallContext;
    const std::shared_ptr<TReadBlocksLocalRequest> Request;
    const NWilson::TTraceId TraceId;

    size_t TryNumber = 0;

    NThreading::TPromise<TReadResponse> Promise =
        NThreading::NewPromise<TReadResponse>();
};

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
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , Promise(NThreading::NewPromise<TResponse>())
{
    SubRequests.reserve(readHint.RangeHints.size());

    for (auto& hint: readHint.RangeHints) {
        // Вычислить смещение в Sglist
        const size_t offsetBlocks = hint.RequestRelativeRange.Start;
        const size_t offsetBytes = offsetBlocks * DefaultBlockSize;
        const size_t sizeBytes =
            hint.RequestRelativeRange.Size() * DefaultBlockSize;

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
        auto executor = std::make_shared<TReadSingleLocationRequestExecutor>(
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

void TReadRequestExecutor::Run()
{
    for (size_t i = 0; i < SubRequests.size(); ++i) {
        auto future = SubRequests[i].Executor->GetFuture();
        future.Subscribe(
            [self = shared_from_this(),
             i](const NThreading::TFuture<TResponse>& f)
            {
                Y_UNUSED(f);
                self->OnSubRequestComplete(i);
            });

        SubRequests[i].Executor->Run();
    }
}

void TReadRequestExecutor::OnSubRequestComplete(size_t index)
{
    const auto& response = SubRequests[index].Executor->GetFuture().GetValue();

    if (HasError(response.Error)) {
        // Первая ошибка - завершить весь запрос
        if (Promise.TrySetValue(response)) {
            // Успешно установили ошибку
            LOG_ERROR(
                *ActorSystem,
                NKikimrServices::NBS_PARTITION,
                "TReadRequestExecutor: SubRequest %zu failed: %s",
                index,
                FormatError(response.Error).c_str());
        }
        return;
    }

    // Проверить, все ли подзапросы завершены
    if (++CompletedCount == SubRequests.size()) {
        // Все успешно
        Promise.SetValue(TResponse{.Error = MakeError(S_OK)});
    }
}

NThreading::TFuture<TReadRequestExecutor::TResponse>
TReadRequestExecutor::GetFuture()
{
    return Promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

TReadSingleLocationRequestExecutor::TReadSingleLocationRequestExecutor(
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

    std::optional<ELocation> location =
        hint.LocationMask.GetLocation(TryNumber);
    if (!location) {
        TString error = TStringBuilder()
                        << "Can't read. Mask:" << hint.LocationMask.Print()
                        << " try:" << TryNumber;
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TReadSingleLocationRequestExecutor %s %s",
            hint.VChunkRange.Print().c_str(),
            error.c_str());

        Reply(MakeError(E_REJECTED, error));
        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TReadSingleLocationRequestExecutor. Reading from location %s",
        ToString(*location).c_str());

    auto onReadResponse = [self = shared_from_this()]   //
        (const NThreading::TFuture<TDBGReadBlocksResponse>& f)
    {
        self->OnReadResponse(f.GetValue());
    };

    auto future = IsDDisk(*location) ? DirectBlockGroup->ReadBlocksFromDDisk(
                                           VChunkConfig.VChunkIndex,
                                           VChunkConfig.GetHostIndex(*location),
                                           hint.VChunkRange,
                                           Request->Sglist,
                                           TraceId)
                                     : DirectBlockGroup->ReadBlocksFromPBuffer(
                                           VChunkConfig.VChunkIndex,
                                           VChunkConfig.GetHostIndex(*location),
                                           hint.Lsn,
                                           hint.VChunkRange,
                                           Request->Sglist,
                                           TraceId);
    future.Subscribe(std::move(onReadResponse));
}

NThreading::TFuture<TReadResponse>
TReadSingleLocationRequestExecutor::GetFuture() const
{
    return Promise.GetFuture();
}

void TReadSingleLocationRequestExecutor::OnReadResponse(
    const TDBGReadBlocksResponse& response)
{
    if (!HasError(response.Error)) {
        Reply(response.Error);
        return;
    }

    LOG_INFO(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "TReadSingleLocationRequestExecutor: OnReadResponse failed %d trying. "
        "Error: %s",
        TryNumber,
        FormatError(response.Error).c_str());

    ++TryNumber;
    Run();
}

void TReadSingleLocationRequestExecutor::Reply(NProto::TError error)
{
    Promise.TrySetValue(TReadResponse{.Error = std::move(error)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
