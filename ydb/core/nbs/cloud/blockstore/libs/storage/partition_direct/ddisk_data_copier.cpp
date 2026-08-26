#include "ddisk_data_copier.h"

#include "partition_direct_service.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/trace_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/model/disk_description.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/format.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr auto MinBackoff = TDuration::MilliSeconds(100);
constexpr auto MaxBackoff = TDuration::Seconds(10);

TBlockRange64 TrimRange(TBlockRange64 range, size_t maxBlockCount)
{
    if (range.Size() > maxBlockCount) {
        return TBlockRange64::WithLength(range.Start, maxBlockCount);
    }
    return range;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TDDiskDataCopier::TCopyRangeRequestState
{
    ui64 SyncId;
    TBlockRange64 Range;
    TRangeLock Lock;
    TString Data;
    NWilson::TSpan Span;

    TCopyRangeRequestState(
        ui64 syncId,
        TBlockRange64 range,
        TRangeLock lock,
        NWilson::TSpan span)
        : SyncId(syncId)
        , Range(range)
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
    ITraceService* traceService,
    IPartitionDirectService* partitionDirectService,
    const TDiskDescription& diskDescription,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TBlocksDirtyMapPtr dirtyMap,
    THostIndex destination)
    : ActorSystem(actorSystem)
    , TraceService(traceService)
    , VChunkConfig(vChunkConfig)
    , VolumeConfig(partitionDirectService->GetVolumeConfig())
    , DirectBlockGroup(std::move(directBlockGroup))
    , Destination(destination)
    , DirtyMap(std::move(dirtyMap))
    , LogTitle{
          GetCycleCount(),
          TLogTitle::TDDiskDataCopier{
              .DiskId = diskDescription.DiskId,
              .TabletId = diskDescription.TabletId,
              .Generation = diskDescription.Generation,
              .DBGIndex = VChunkConfig.GetDBGIndex(),
              .VChunkIndex = VChunkConfig.GetVChunkIndex(),
              .Destination = static_cast<int>(Destination)}}
    , BackoffDelayProvider(MinBackoff, MaxBackoff)
{
    Y_ABORT_UNLESS(traceService);
    Y_ABORT_UNLESS(Destination < VChunkConfig.GetHostCount());
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

    Complete = NewPromise<EResult>();
    StartCopyRange();
    return Complete.GetFuture();
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

std::optional<TBlockRange64> TDDiskDataCopier::GetFreshRange() const
{
    auto freshRange = DirtyMap->GetFreshRange(Destination);
    if (!freshRange) {
        return std::nullopt;
    }

    return TrimRange(*freshRange, CopyRangeSize / VolumeConfig->BlockSize);
}

NWilson::TSpan TDDiskDataCopier::CreateSpan(TBlockRange64 range) const
{
    auto span = TraceService->CreateRootSpan("CopyRange");
    span.Attribute("DiskId", VolumeConfig->DiskId);
    span.Attribute("From", static_cast<i64>(range.Start));
    span.Attribute(
        "Length",
        static_cast<i64>(range.Size() * VolumeConfig->BlockSize));
    return span;
}

void TDDiskDataCopier::StartCopyRange()
{
    auto freshRange = GetFreshRange();

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s Will copy range: %s",
        LogTitle.GetWithTime().c_str(),
        freshRange ? freshRange->Print().c_str() : "<empty>");

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
            if (!freshRange) {
                State = EState::Stopped;
                Complete.SetValue(EResult::Ok);
                return;
            }
            break;
    }

    auto timeWaitBeforeExecution = DirectBlockGroup->TakeCopyRangeBudget(
        freshRange->Size() * VolumeConfig->BlockSize);
    auto hint = DirtyMap->BeginRangeSync(Destination, *freshRange);
    hint.ReadyToStart.Subscribe(
        [weakSelf = weak_from_this(),
         syncId = hint.SyncId,
         range = hint.Range,
         willStartAt = TInstant::Now() + timeWaitBeforeExecution]   //
        (const TFuture<void>& f) mutable
        {
            Y_UNUSED(f);

            if (auto self = weakSelf.lock()) {
                self->CopyRange(willStartAt - TInstant::Now(), syncId, range);
            }
        });
}

void TDDiskDataCopier::CopyRange(
    TDuration timeWaitBeforeExecution,
    ui64 syncId,
    TBlockRange64 range)
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

    auto copyRangeState = std::make_shared<TCopyRangeRequestState>(
        syncId,
        range,
        TRangeLock(DirtyMap, range, THostMask::MakeOne(Destination)),
        CreateSpan(range));

    auto readHint = DirtyMap->MakeReadHint(range);
    if (readHint.RangeHints.empty()) {
        DirtyMap->EndRangeSync(copyRangeState->SyncId, false);
        auto waitReadyFuture = readHint.WaitReady;
        Y_ABORT_UNLESS(!waitReadyFuture.HasValue());
        waitReadyFuture.Subscribe(
            [weakSelf = weak_from_this()]   //
            (const NThreading::TFuture<void>& f)
            {
                Y_UNUSED(f);

                if (auto self = weakSelf.lock()) {
                    self->StartCopyRange();
                }
            });
        return;
    }

    const ui64 requestId = Random();
    std::shared_ptr<TReadBlocksLocalRequest> readRequest =
        std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
            .VolumeConfig = VolumeConfig,
            .RequestId = requestId,
            .Range = range,
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

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s %lu %s Read: %s",
        LogTitle.GetWithTime().c_str(),
        copyRangeState->SyncId,
        copyRangeState->Range.Print().c_str(),
        FormatError(response.Error).Quote().c_str());

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s %lu %s Read error: %s",
            LogTitle.GetWithTime().c_str(),
            copyRangeState->SyncId,
            copyRangeState->Range.Print().c_str(),
            FormatError(response.Error).Quote().c_str());

        DirtyMap->EndRangeSync(copyRangeState->SyncId, false);
        if (IsNeverRetriableError(response.Error)) {
            Complete.SetValue(EResult::Error);
        } else {
            ScheduleStartCopyRange(BackoffDelayProvider.GetDelayAndIncrease());
        }
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

    LOG_DEBUG(
        *ActorSystem,
        NKikimrServices::NBS_PARTITION,
        "%s %lu %s Write: %s",
        LogTitle.GetWithTime().c_str(),
        copyRangeState->SyncId,
        copyRangeState->Range.Print().c_str(),
        FormatError(response.Error).Quote().c_str());

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s %lu %s Write error: %s",
            LogTitle.GetWithTime().c_str(),
            copyRangeState->SyncId,
            copyRangeState->Range.Print().c_str(),
            FormatError(response.Error).Quote().c_str());

        DirtyMap->EndRangeSync(copyRangeState->SyncId, false);
        if (IsNeverRetriableError(response.Error)) {
            Complete.SetValue(EResult::Error);
        } else {
            ScheduleStartCopyRange(BackoffDelayProvider.GetDelayAndIncrease());
        }
        return;
    }

    BackoffDelayProvider.Reset();
    DirtyMap->EndRangeSync(copyRangeState->SyncId, true);
    StartCopyRange();
}

void TDDiskDataCopier::ScheduleStartCopyRange(TDuration delay)
{
    DirectBlockGroup->Schedule(
        delay,
        [weakSelf = weak_from_this()]()
        {
            if (auto self = weakSelf.lock()) {
                self->StartCopyRange();
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
