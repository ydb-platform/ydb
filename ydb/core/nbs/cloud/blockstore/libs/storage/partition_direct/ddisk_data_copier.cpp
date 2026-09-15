#include "ddisk_data_copier.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

<<<<<<< HEAD
struct TDDiskDataCopier::TCopyRangeRequestState
{
    TBlockRange64 Range;
=======
namespace {

constexpr auto MinBackoff = TDuration::MilliSeconds(100);
constexpr auto MaxBackoff = TDuration::Seconds(10);

TBlockRange16 TrimRange(TBlockRange16 range, ui16 maxBlockCount)
{
    if (range.Size() > maxBlockCount) {
        return TBlockRange16::WithLength(range.Start, maxBlockCount);
    }
    return range;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TDDiskDataCopier::TCopyRangeRequestState
{
    ui64 SyncId;
    TBlockRange16 Range;
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    TRangeLock Lock;
    TString Data;
    NWilson::TSpan Span;

    TCopyRangeRequestState(
<<<<<<< HEAD
        TBlockRange64 range,
=======
        ui64 syncId,
        TBlockRange16 range,
        ui32 blockSize,
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
        TRangeLock lock,
        NWilson::TSpan span)
        : Range(range)
        , Lock(std::move(lock))
        , Span(std::move(span))
    {
        Data.resize(CopyRangeSize);
        Lock.Arm();
    }

    TGuardedSgList GetSgList() const
    {
        return TGuardedSgList({TBlockDataRef(Data.data(), Data.size())});
    }
};

////////////////////////////////////////////////////////////////////////////////

TDDiskDataCopier::TDDiskDataCopier(
    NActors::TActorSystem* actorSystem,
    IPartitionDirectService* partitionDirectService,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TBlocksDirtyMap* dirtyMap,
    THostIndex destination)
    : ActorSystem(actorSystem)
    , PartitionDirectService(partitionDirectService)
    , VChunkConfig(vChunkConfig)
    , VolumeConfig(partitionDirectService->GetVolumeConfig())
    , DirectBlockGroup(std::move(directBlockGroup))
    , Destination(destination)
    , DirtyMap(dirtyMap)
    , LogTitle{
          GetCycleCount(),
          TLogTitle::TDDiskDataCopier{
              .DiskId = VolumeConfig->DiskId,
              .Destination = static_cast<int>(Destination)}}
{
    Y_ASSERT(Destination < VChunkConfig.GetHostCount());
}

TFuture<TDDiskDataCopier::EResult> TDDiskDataCopier::Start()
{
    switch (State) {
        case EState::Stopped: {
            State = EState::Running;
            break;
        }
        case EState::Stopping: {
            State = EState::Running;
            return Complete.GetFuture();
        }
        case EState::Running: {
            return Complete.GetFuture();
        }
    }

    if (auto watermark = DirtyMap->GetFreshWatermark(Destination)) {
        FreshWatermark = *watermark;
        Complete = NewPromise<EResult>();
        StartCopyRange();
        return Complete.GetFuture();
    }

    State = EState::Stopped;
    return MakeFuture(EResult::Ok);
}

TFuture<TDDiskDataCopier::EResult> TDDiskDataCopier::Stop()
{
    switch (State) {
        case EState::Stopped: {
            return MakeFuture(EResult::Ok);
        }
        case EState::Stopping: {
            return Complete.GetFuture();
        }
        case EState::Running: {
            State = EState::Stopping;
            return Complete.GetFuture();
        }
    }
}

NWilson::TSpan TDDiskDataCopier::CreateSpan() const
{
<<<<<<< HEAD
    auto span = PartitionDirectService->CreteRootSpan("CopyRange");
=======
    return BytesCopied;
}

std::optional<TBlockRange16> TDDiskDataCopier::GetFreshRange() const
{
    auto freshRange = Client->GetFreshRange(Destination);
    if (!freshRange) {
        return std::nullopt;
    }

    return TrimRange(
        *freshRange,
        IntegerCast<ui16>(CopyRangeSize / VolumeConfig->BlockSize));
}

NWilson::TSpan TDDiskDataCopier::CreateSpan(TBlockRange16 range) const
{
    auto span = TraceService->CreateRootSpan("CopyRange");
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))
    span.Attribute("DiskId", VolumeConfig->DiskId);
    span.Attribute("From", static_cast<i64>(FreshWatermark));
    span.Attribute("Length", static_cast<i64>(CopyRangeSize));
    return span;
}

void TDDiskDataCopier::StartCopyRange()
{
    switch (State) {
        case EState::Stopped: {
            Y_ABORT_UNLESS(false);
            break;
        }
        case EState::Stopping: {
            State = EState::Stopped;
            Complete.SetValue(EResult::Interrupted);
            return;
        }
        case EState::Running:
            break;
    }

    Y_ABORT_UNLESS(
        DirtyMap->GetFreshWatermark(Destination) &&
        FreshWatermark == DirtyMap->GetFreshWatermark(Destination));

<<<<<<< HEAD
    const ui64 futureWatermark = FreshWatermark + CopyRangeSize;
    auto range = TBlockRange64::WithLength(
        FreshWatermark / VolumeConfig->BlockSize,
        CopyRangeSize / VolumeConfig->BlockSize);
=======
            if (auto self = weakSelf.lock()) {
                self->CopyRange(willStartAt - TInstant::Now(), syncId, range);
            }
        });
}

void TDDiskDataCopier::CopyRange(
    TDuration timeWaitBeforeExecution,
    ui64 syncId,
    TBlockRange16 range)
{
    if (timeWaitBeforeExecution) {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s %lu %s Schedule copy range %s",
            LogTitle.GetWithTime().c_str(),
            syncId,
            range.Print().c_str(),
            FormatDuration(timeWaitBeforeExecution).c_str());

        DirectBlockGroup->Schedule(
            timeWaitBeforeExecution,
            [weakSelf = weak_from_this(), syncId, range]()
            {
                if (auto self = weakSelf.lock()) {
                    self->CopyRange({}, syncId, range);
                }
            });

        return;
    }

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s %lu %s Copy range",
        LogTitle.GetWithTime().c_str(),
        syncId,
        range.Print().c_str());
>>>>>>> 68783cc737c ([NBS-7725] Optimize inflight size (#53015))

    auto copyRangeState = std::make_shared<TCopyRangeRequestState>(
        range,
        TRangeLock(DirtyMap, range, THostMask::MakeOne(Destination)),
        CreateSpan());

    DirtyMap->SetFlushWatermark(Destination, futureWatermark);

    auto readHint = DirtyMap->MakeReadHint(range);
    Y_ABORT_UNLESS(!readHint.RangeHints.empty());

    const ui64 requestId = Random();
    std::shared_ptr<TReadBlocksLocalRequest> readRequest =
        std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
            .VolumeConfig = VolumeConfig,
            .RequestId = requestId,
            .Range = TBlockRange64::WithLength(0, range.Size()),
            .Timestamp = TInstant::Now()});
    readRequest->Sglist = copyRangeState->GetSgList();
    auto callContext = MakeIntrusive<TCallContext>(requestId);
    callContext->RootTraceId = copyRangeState->Span.GetTraceId();

    auto readExecutor = CreateReadRequestExecutor(
        ActorSystem,
        LogTitle,
        VChunkConfig,
        DirectBlockGroup,
        std::move(readHint),
        std::move(callContext),
        std::move(readRequest),
        NWilson::TTraceId());

    auto future = readExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         copyRangeState = std::move(copyRangeState)]   //
        (const TFuture<IReadRequestExecutor::TResponse>& f) mutable
        {
            if (auto self = weakSelf.lock()) {
                self->OnRangeRead(std::move(copyRangeState), f.GetValue());
            }
        });
    readExecutor->Run();
}

void TDDiskDataCopier::OnRangeRead(
    TCopyRangeRequestStatePtr copyRangeState,
    const IReadRequestExecutor::TResponse& response)
{
    copyRangeState->Span.Event("OnRangeRead");

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TDDiskDataCopier. %s Read error: %s",
            copyRangeState->Range.Print().c_str(),
            FormatError(response.Error).c_str());

        Complete.SetValue(EResult::Error);
        return;
    }

    auto writeFuture = DirectBlockGroup->WriteBlocksToDDisk(
        VChunkConfig.GetVChunkIndex(),
        Destination,
        copyRangeState->Range,
        copyRangeState->GetSgList(),
        NWilson::TTraceId());
    auto l = [weakSelf = weak_from_this(),
              copyRangeState = std::move(copyRangeState)]   //
        (const NThreading::TFuture<TDBGWriteBlocksResponse>& f) mutable
    {
        if (auto self = weakSelf.lock()) {
            self->OnRangeWritten(std::move(copyRangeState), f.GetValue());
        }
    };
    writeFuture.Subscribe(std::move(l));
}

void TDDiskDataCopier::OnRangeWritten(
    TCopyRangeRequestStatePtr copyRangeState,
    const TDBGWriteBlocksResponse& response)
{
    copyRangeState->Span.Event("OnRangeWritten");

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TDDiskDataCopier. %s Write error: %s",
            copyRangeState->Range.Print().c_str(),
            FormatError(response.Error).c_str());

        Complete.SetValue(EResult::Error);
        return;
    }

    FreshWatermark = (copyRangeState->Range.End + 1) * VolumeConfig->BlockSize;
    DirtyMap->SetReadWatermark(Destination, FreshWatermark);
    Y_ABORT_UNLESS(VolumeConfig->VChunkSize > 0);
    if (FreshWatermark < VolumeConfig->VChunkSize) {
        StartCopyRange();
    } else {
        Complete.SetValue(EResult::Ok);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
