#include "ddisk_data_copier.h"

#include "read_request.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/partition_direct_service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TGuardedSgList TDDiskDataCopier::TCopyRangeRequestState::GetSgList() const
{
    return TGuardedSgList({TBlockDataRef(Data.data(), Data.size())});
}

////////////////////////////////////////////////////////////////////////////////

TDDiskDataCopier::TDDiskDataCopier(
    NActors::TActorSystem* actorSystem,
    const TVChunkConfig& vChunkConfig,
    IPartitionDirectServicePtr partitionDirectService,
    IDirectBlockGroupPtr directBlockGroup,
    TBlocksDirtyMap* dirtyMap,
    ELocation destination)
    : ActorSystem(actorSystem)
    , VChunkConfig(vChunkConfig)
    , VolumeConfig(partitionDirectService->GetVolumeConfig())
    , PartitionDirectService(std::move(partitionDirectService))
    , DirectBlockGroup(std::move(directBlockGroup))
    , Destination(destination)
    , DirtyMap(dirtyMap)
{}

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
    auto span = PartitionDirectService->CreteRootSpan("CopyRange");
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

    const ui64 futureWatermark = FreshWatermark + CopyRangeSize;
    auto range = TBlockRange64::WithLength(
        FreshWatermark / VolumeConfig->BlockSize,
        CopyRangeSize / VolumeConfig->BlockSize);

    TCopyRangeRequestStatePtr state = std::make_shared<TCopyRangeRequestState>(
        range,
        TRangeLock(DirtyMap, range, TLocationMask::MakeOne(Destination)));
    state->Span = CreateSpan();
    state->Lock.Arm();
    state->Data.resize(CopyRangeSize);

    DirtyMap->SetFlushWatermark(Destination, futureWatermark);

    auto readHint = DirtyMap->MakeReadHint(range);
    Y_ABORT_UNLESS(!readHint.RangeHints.empty());

    const ui64 requestId = Random();
    std::shared_ptr<TReadBlocksLocalRequest> readRequest =
        std::make_shared<TReadBlocksLocalRequest>(TRequestHeaders{
            .VolumeConfig = VolumeConfig,
            .RequestId = requestId,
            .Range = range,
            .Timestamp = TInstant::Now()});
    readRequest->Sglist = state->GetSgList();
    auto callContext = MakeIntrusive<TCallContext>(requestId);
    callContext->RootTraceId = state->Span.GetTraceId();

    auto readExecutor = std::make_shared<TReadRequestExecutor>(
        ActorSystem,
        VChunkConfig,
        DirectBlockGroup,
        std::move(readHint),
        std::move(callContext),
        std::move(readRequest),
        NWilson::TTraceId());

    auto future = readExecutor->GetFuture();
    future.Subscribe(
        [weakSelf = weak_from_this(),
         state = std::move(state)]   //
        (const TFuture<TReadRequestExecutor::TResponse>& f) mutable
        {
            if (auto self = weakSelf.lock()) {
                self->OnRangeRead(std::move(state), f.GetValue());
            }
        });
    readExecutor->Run();
}

void TDDiskDataCopier::OnRangeRead(
    TCopyRangeRequestStatePtr state,
    const TReadRequestExecutor::TResponse& response)
{
    state->Span.Event("OnRangeRead");

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TDDiskDataCopier. %s Read error: %s",
            state->Range.Print().c_str(),
            FormatError(response.Error).c_str());

        Complete.SetValue(EResult::Error);
        return;
    }

    auto writeFuture = DirectBlockGroup->WriteBlocksToDDisk(
        VChunkConfig.VChunkIndex,
        VChunkConfig.GetHostIndex(Destination),
        state->Range,
        state->GetSgList(),
        NWilson::TTraceId());
    auto l = [weakSelf = weak_from_this(),
              state = std::move(state)]   //
        (const NThreading::TFuture<TDBGWriteBlocksResponse>& f) mutable
    {
        if (auto self = weakSelf.lock()) {
            self->OnRangeWritten(std::move(state), f.GetValue());
        }
    };
    writeFuture.Subscribe(std::move(l));
}

void TDDiskDataCopier::OnRangeWritten(
    TCopyRangeRequestStatePtr state,
    const TDBGWriteBlocksResponse& response)
{
    state->Span.Event("OnRangeWritten");

    if (HasError(response.Error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "TDDiskDataCopier. %s Write error: %s",
            state->Range.Print().c_str(),
            FormatError(response.Error).c_str());

        Complete.SetValue(EResult::Error);
        return;
    }

    FreshWatermark = (state->Range.End + 1) * VolumeConfig->BlockSize;
    DirtyMap->SetReadWatermark(Destination, FreshWatermark);
    if (FreshWatermark < VChunkSize) {
        StartCopyRange();
    } else {
        Complete.SetValue(EResult::Ok);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
