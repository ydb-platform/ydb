#include "read_request_multiple_location.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/block_checksums.h>
#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/future_helper.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#include <algorithm>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TReadMultipleLocationRequestExecutor::TReadMultipleLocationRequestExecutor(
    NActors::TActorSystem const* actorSystem,
    const TLogTitle& logTitle,
    const TVChunkConfig& vChunkConfig,
    IDirectBlockGroupPtr directBlockGroup,
    TReadHint readHint,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
    : ActorSystem(actorSystem)
    , LogTitle(logTitle.GetChildWithTags(
          GetCycleCount(),
          {{"t", "MultiRead"}, {"r", request->Headers.Range}}))
    , VChunkConfig(vChunkConfig)
    , DirectBlockGroup(std::move(directBlockGroup))
    , CallContext(std::move(callContext))
    , Request(std::move(request))
    , TraceId(std::move(traceId))
    , SgList(Request->Sglist.CreateDepender())
{
    Y_ASSERT(Request->Headers.VolumeConfig);
    Y_ASSERT(Request->Headers.VolumeConfig->BlockSize != 0);

    const size_t blockSize = Request->Headers.VolumeConfig->BlockSize;
    Y_ABORT_UNLESS(blockSize % ChecksumUnitSize == 0);

    auto guard = SgList.Acquire();
    if (!guard) {
        Reply(MakeCanNotAcquireDataError(), {}, 0);
        return;
    }

    ParentChecksumCount = static_cast<size_t>(Request->Headers.Range.Size()) *
                          blockSize / ChecksumUnitSize;

    SubRequestExecutors.reserve(readHint.RangeHints.size());
    SubRequestChecksumPlaces.reserve(readHint.RangeHints.size());
    for (auto& hint: readHint.RangeHints) {
        // Compute offset for Sglist
        const size_t offsetBlocks = hint.RequestRelativeRange.Start;
        const size_t offsetBytes = offsetBlocks * blockSize;
        const size_t sizeBytes = hint.RequestRelativeRange.Size() * blockSize;
        const TSubRequestChecksumPlace place{
            .Offset = offsetBytes / ChecksumUnitSize,
            .Count = sizeBytes / ChecksumUnitSize};
        Y_ABORT_UNLESS(place.Offset + place.Count <= ParentChecksumCount);
        SubRequestChecksumPlaces.push_back(place);

        auto subRequest =
            std::make_shared<TReadBlocksLocalRequest>(Request->Headers.Clone(
                ConvertRangeSafe<TBlockRange64>(hint.VChunkRange)));

        // Create subbuffer Sglist for current range
        subRequest->Sglist = SgList.CreateDepender(
            CreateSgListSubRange(guard.Get(), offsetBytes, sizeBytes));

        auto executor = std::make_shared<TReadSingleLocationRequestExecutor>(
            ActorSystem,
            logTitle,
            VChunkConfig,
            DirectBlockGroup,
            std::move(hint),
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
        future.Subscribe([self = shared_from_this(), i]   //
                         (const NThreading::TFuture<TResponse>& f)
                         {   //
                             self->OnSubRequestComplete(f.GetValue(), i);
                         });

        SubRequestExecutors[i]->Run();
    }
}

TString TReadMultipleLocationRequestExecutor::Print()
{
    TStringBuilder result;
    result << LogTitle.GetWithTime();
    result << " Subrequests: " << CompletedCount << "/"
           << SubRequestExecutors.size();
    result << (Promise.IsReady() ? " Replied" : "Not replied");
    return result;
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
        Reply(response.Error, {}, index);
        return;
    }

    AcceptSubRequestChecksums(response.Checksums, index);

    if (CompletedCount == SubRequestExecutors.size()) {
        // Every piece is empty (checksums disabled) or every piece is
        // complete (checksums enabled). A mix means the DBG gate is broken.
        Y_ABORT_UNLESS(
            AssembledChecksumCount == 0 ||
            AssembledChecksumCount == ParentChecksumCount);
        Reply(MakeError(S_OK), std::move(AssembledChecksums), index);
    }
}

void TReadMultipleLocationRequestExecutor::AcceptSubRequestChecksums(
    const TBlockChecksums& checksums,
    size_t index)
{
    Y_ABORT_UNLESS(index < SubRequestChecksumPlaces.size());
    const TSubRequestChecksumPlace place = SubRequestChecksumPlaces[index];
    // Empty means checksums are disabled. Anything else must be exactly one
    // value per ChecksumUnitSize bytes of this piece.
    Y_ABORT_UNLESS(checksums.empty() || checksums.size() == place.Count);
    if (checksums.empty()) {
        return;
    }

    if (AssembledChecksums.empty()) {
        AssembledChecksums.resize(ParentChecksumCount);
    }

    std::copy(
        checksums.begin(),
        checksums.end(),
        AssembledChecksums.begin() + place.Offset);
    AssembledChecksumCount += checksums.size();
}

void TReadMultipleLocationRequestExecutor::Reply(
    NProto::TError error,
    TBlockChecksums checksums,
    size_t index)
{
    if (Promise.IsReady()) {
        return;
    }

    if (HasError(error)) {
        LOG_ERROR(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s SubRequest: %zu, Error: %s",
            LogTitle.GetWithTime().c_str(),
            index,
            FormatError(error).Quote().c_str());
    } else {
        LOG_DEBUG(
            *ActorSystem,
            NKikimrServices::NBS_PARTITION,
            "%s OK",
            LogTitle.GetWithTime().c_str());
    }

    SgList.Close();

    Y_DEBUG_ABORT_UNLESS(!HasError(error) || checksums.empty());

    Promise.TrySetValue(TResponse{
        .Error = std::move(error),
        .Checksums = std::move(checksums)});
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
