#include "aligned_device_handler.h"

#include <ydb/core/nbs/cloud/blockstore/libs/service/context.h>
#include <ydb/core/nbs/cloud/blockstore/libs/service/storage.h>

#include <util/string/builder.h>

namespace NYdb::NBS::NBlockStore {

using namespace NThreading;

namespace {

TErrorResponse CreateErrorAcquireResponse()
{
    return {E_CANCELLED, "failed to acquire sglist in DeviceHandler"};
}

TErrorResponse CreateRequestNotAlignedResponse()
{
    return {E_ARGUMENT, "Request is not aligned"};
}

// Removes the first blockCount elements from the sgList. Returns these removed
// items in TGuardedSgList.
TGuardedSgList TakeHeadBlocks(TGuardedSgList& sgList, ui32 blockCount)
{
    auto guard = sgList.Acquire();
    if (!guard) {
        return {};
    }

    const TSgList& blockList = guard.Get();
    auto result =
        sgList.Create({blockList.begin(), blockList.begin() + blockCount});
    sgList.SetSgList({blockList.begin() + blockCount, blockList.end()});
    return result;
}

}   // namespace

NProto::TError TryToNormalize(
    TGuardedSgList& guardedSgList,
    TBlocksInfo& blocksInfo)
{
    const auto length = blocksInfo.BufferSize();
    if (length == 0) {
        return MakeError(E_ARGUMENT, "Local request has zero length");
    }

    auto guard = guardedSgList.Acquire();
    if (!guard) {
        return CreateErrorAcquireResponse();
    }

    auto bufferSize = SgListGetSize(guard.Get());
    if (bufferSize != length) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Invalid local request: buffer size " << bufferSize
                << " not equal to length " << length);
    }

    if (!blocksInfo.IsAligned()) {
        return MakeError(S_OK);
    }

    bool allBuffersAligned = AllOf(
        guard.Get(),
        [blockSize = blocksInfo.BlockSize](const auto& buffer)
        { return buffer.Size() % blockSize == 0; });

    if (!allBuffersAligned) {
        blocksInfo.SgListAligned = false;
        return MakeError(S_OK);
    }

    auto sgListOrError = SgListNormalize(guard.Get(), blocksInfo.BlockSize);
    if (HasError(sgListOrError)) {
        return sgListOrError.GetError();
    }

    guardedSgList.SetSgList(sgListOrError.ExtractResult());
    return MakeError(S_OK);
}

////////////////////////////////////////////////////////////////////////////////

TAlignedDeviceHandler::TAlignedDeviceHandler(
    TDeviceHandlerParams params,
    ui32 maxSubRequestSize)
    : Storage(std::move(params.Storage))
    , DiskId(std::move(params.DiskId))
    , ClientId(std::move(params.ClientId))
    , BlockSize(params.BlockSize)
    , MaxBlockCount(maxSubRequestSize / BlockSize)
    , MaxBlockCountForZeroBlocksRequest(
          params.MaxZeroBlocksSubRequestSize / BlockSize)
{
    Y_ABORT_UNLESS(MaxBlockCount > 0);
    Y_ABORT_UNLESS(MaxBlockCountForZeroBlocksRequest > 0);
}

TFuture<TReadBlocksLocalResponse> TAlignedDeviceHandler::Read(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList,
    const TString& checkpointId)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<TReadBlocksLocalResponse>(
            TReadBlocksLocalResponse{.Error = normalizeError});
    }

    if (!blocksInfo.IsAligned()) {
        return MakeFuture<TReadBlocksLocalResponse>(TReadBlocksLocalResponse{
            .Error = CreateRequestNotAlignedResponse()});
    }

    return ExecuteReadRequest(
        std::move(ctx),
        blocksInfo,
        std::move(sgList),
        checkpointId);
}

TFuture<TWriteBlocksLocalResponse> TAlignedDeviceHandler::Write(
    TCallContextPtr ctx,
    ui64 from,
    ui64 length,
    TGuardedSgList sgList)
{
    auto blocksInfo = TBlocksInfo(from, length, BlockSize);

    auto normalizeError = TryToNormalize(sgList, blocksInfo);
    if (HasError(normalizeError)) {
        return MakeFuture<TWriteBlocksLocalResponse>(
            TWriteBlocksLocalResponse{.Error = normalizeError});
    }

    if (!blocksInfo.IsAligned()) {
        return MakeFuture<TWriteBlocksLocalResponse>(TWriteBlocksLocalResponse{
            .Error = CreateRequestNotAlignedResponse()});
    }

    return ExecuteWriteRequest(std::move(ctx), blocksInfo, std::move(sgList));
}

TFuture<TZeroBlocksLocalResponse>
TAlignedDeviceHandler::Zero(TCallContextPtr ctx, ui64 from, ui64 length)
{
    if (length == 0) {
        return MakeFuture<TZeroBlocksLocalResponse>(TZeroBlocksLocalResponse{
            .Error = MakeError(E_ARGUMENT, "Local request has zero length")});
    }

    auto blocksInfo = TBlocksInfo(from, length, BlockSize);
    if (!blocksInfo.IsAligned()) {
        return MakeFuture<TZeroBlocksLocalResponse>(TZeroBlocksLocalResponse{
            .Error = CreateRequestNotAlignedResponse()});
    }

    return ExecuteZeroRequest(std::move(ctx), blocksInfo);
}

TFuture<TReadBlocksLocalResponse> TAlignedDeviceHandler::ExecuteReadRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList,
    TString checkpointId)
{
    Y_DEBUG_ABORT_UNLESS(blocksInfo.IsAligned());

    const ui32 requestBlockCount =
        std::min<ui32>(blocksInfo.Range.Size(), MaxBlockCount);

    auto request = std::make_shared<TReadBlocksLocalRequest>(
        TRequestHeaders{
            .RequestId = ctx->RequestId,
            .ClientId = ClientId,
            .Timestamp = TInstant::Now()},
        TBlockRange64::WithLength(blocksInfo.Range.Start, requestBlockCount));

    if (requestBlockCount == blocksInfo.Range.Size()) {
        // The request size is quite small. We do all work at once.
        request->Sglist = std::move(sgList);
        auto result =
            Storage->ReadBlocksLocal(std::move(ctx), std::move(request));
        return result.Subscribe(
            [weakPtr = weak_from_this(), range = blocksInfo.Range](
                const TFuture<TReadBlocksLocalResponse>& future)
            {
                const auto& response = future.GetValue();
                if (HasError(response.Error)) {
                    if (auto self = weakPtr.lock()) {
                        self->ReportCriticalError(
                            response.Error,
                            "Read",
                            range);
                    }
                }
            });
    }

    // Take the list of blocks that we will execute in the first
    // sub-request and leave the rest in original sgList.
    request->Sglist = TakeHeadBlocks(sgList, requestBlockCount);
    if (request->Sglist.Empty()) {
        return MakeFuture<TReadBlocksLocalResponse>(
            {.Error = CreateErrorAcquireResponse()});
    }

    auto result = Storage->ReadBlocksLocal(ctx, std::move(request));

    auto originalRange = blocksInfo.Range;
    blocksInfo.Range = TBlockRange64::WithLength(
        blocksInfo.Range.Start + requestBlockCount,
        blocksInfo.Range.Size() - requestBlockCount);
    Y_DEBUG_ABORT_UNLESS(blocksInfo.Range.Size());

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         sgList = std::move(sgList),
         checkpointId = std::move(checkpointId),
         originalRange = originalRange](
            const TFuture<TReadBlocksLocalResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response.Error)) {
                if (auto self = weakPtr.lock()) {
                    self->ReportCriticalError(
                        response.Error,
                        "Read",
                        originalRange);
                }
                return future;
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteReadRequest(
                    std::move(ctx),
                    blocksInfo,
                    std::move(sgList),
                    std::move(checkpointId));
            }
            return MakeFuture<TReadBlocksLocalResponse>(
                {.Error = TErrorResponse(E_CANCELLED)});
        });
}

TFuture<TWriteBlocksLocalResponse> TAlignedDeviceHandler::ExecuteWriteRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo,
    TGuardedSgList sgList)
{
    Y_DEBUG_ABORT_UNLESS(blocksInfo.IsAligned());

    const ui32 requestBlockCount =
        std::min<ui32>(blocksInfo.Range.Size(), MaxBlockCount);

    auto request = std::make_shared<TWriteBlocksLocalRequest>(
        TRequestHeaders{
            .RequestId = ctx->RequestId,
            .ClientId = ClientId,
            .Timestamp = TInstant::Now()},
        TBlockRange64::WithLength(blocksInfo.Range.Start, requestBlockCount));

    if (requestBlockCount == blocksInfo.Range.Size()) {
        // The request size is quite small. We do all work at once.
        request->Sglist = std::move(sgList);
        auto result =
            Storage->WriteBlocksLocal(std::move(ctx), std::move(request));
        return result.Subscribe(
            [weakPtr = weak_from_this(), range = blocksInfo.Range](
                const TFuture<TWriteBlocksLocalResponse>& future)
            {
                const auto& response = future.GetValue();
                if (HasError(response.Error)) {
                    if (auto self = weakPtr.lock()) {
                        self->ReportCriticalError(
                            response.Error,
                            "Write",
                            range);
                    }
                }
            });
    }

    // Take the list of blocks that we will execute in the first
    // sub-request and leave the rest in original sgList.
    request->Sglist = TakeHeadBlocks(sgList, requestBlockCount);
    if (request->Sglist.Empty()) {
        return MakeFuture<TWriteBlocksLocalResponse>(
            {.Error = CreateErrorAcquireResponse()});
    }

    auto result = Storage->WriteBlocksLocal(ctx, std::move(request));

    auto originalRange = blocksInfo.Range;
    blocksInfo.Range = TBlockRange64::WithLength(
        blocksInfo.Range.Start + requestBlockCount,
        blocksInfo.Range.Size() - requestBlockCount);
    Y_DEBUG_ABORT_UNLESS(blocksInfo.Range.Size());

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         sgList = std::move(sgList),
         originalRange = originalRange](
            const TFuture<TWriteBlocksLocalResponse>& future) mutable
        {
            const auto& response = future.GetValue();
            if (HasError(response.Error)) {
                if (auto self = weakPtr.lock()) {
                    self->ReportCriticalError(
                        response.Error,
                        "Write",
                        originalRange);
                }
                return future;
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteWriteRequest(
                    std::move(ctx),
                    blocksInfo,
                    std::move(sgList));
            }
            return MakeFuture<TWriteBlocksLocalResponse>(
                {.Error = TErrorResponse(E_CANCELLED)});
        });
}

TFuture<TZeroBlocksLocalResponse> TAlignedDeviceHandler::ExecuteZeroRequest(
    TCallContextPtr ctx,
    TBlocksInfo blocksInfo)
{
    Y_DEBUG_ABORT_UNLESS(blocksInfo.IsAligned());

    const ui32 requestBlockCount = std::min<ui32>(
        blocksInfo.Range.Size(),
        MaxBlockCountForZeroBlocksRequest);

    auto request = std::make_shared<TZeroBlocksLocalRequest>(
        TRequestHeaders{
            .RequestId = ctx->RequestId,
            .ClientId = ClientId,
            .Timestamp = TInstant::Now()},
        TBlockRange64::WithLength(blocksInfo.Range.Start, requestBlockCount));

    if (requestBlockCount == blocksInfo.Range.Size()) {
        // The request size is quite small. We do all work at once.
        auto result =
            Storage->ZeroBlocksLocal(std::move(ctx), std::move(request));
        return result.Subscribe(
            [weakPtr = weak_from_this(), range = blocksInfo.Range](
                const TFuture<TZeroBlocksLocalResponse>& future)
            {
                const auto& response = future.GetValue();
                if (HasError(response.Error)) {
                    if (auto self = weakPtr.lock()) {
                        self->ReportCriticalError(
                            response.Error,
                            "Zero",
                            range);
                    }
                }
            });
    }

    auto result = Storage->ZeroBlocksLocal(ctx, std::move(request));

    auto originalRange = blocksInfo.Range;
    blocksInfo.Range.Start += requestBlockCount;

    return result.Apply(
        [ctx = std::move(ctx),
         weakPtr = weak_from_this(),
         blocksInfo = blocksInfo,
         originalRange = originalRange](
            const TFuture<TZeroBlocksLocalResponse>& future) mutable
        {
            // Only part of the request was completed. Continue doing the
            // rest of the work

            const auto& response = future.GetValue();
            if (HasError(response.Error)) {
                if (auto self = weakPtr.lock()) {
                    self->ReportCriticalError(
                        response.Error,
                        "Zero",
                        originalRange);
                }
                return future;
            }

            if (auto self = weakPtr.lock()) {
                return self->ExecuteZeroRequest(std::move(ctx), blocksInfo);
            }
            return MakeFuture<TZeroBlocksLocalResponse>(
                {.Error = TErrorResponse(E_CANCELLED)});
        });
}

void TAlignedDeviceHandler::ReportCriticalError(
    const NProto::TError& error,
    const TString& operation,
    TBlockRange64 range)
{
    if (error.GetCode() == E_CANCELLED) {
        // Do not raise crit event when client disconnected.
        // Keep the logic synchronized with blockstore/libs/vhost/server.cpp.
        return;
    }

    if (error.GetCode() == E_IO_SILENT &&
        (error.GetMessage().Contains(
             "Request WriteBlocks is not allowed for client") ||
         error.GetMessage().Contains(
             "Request WriteBlocksLocal is not allowed for client") ||
         error.GetMessage().Contains(
             "Request ZeroBlocks is not allowed for client")))
    {
        // Don't raise crit event when client try to write with read-only mount.
        // See cloud/blockstore/libs/storage/volume/model/client_state.cpp
        return;
    }

    auto message = TStringBuilder()
                   << "disk: " << DiskId.Quote() << ", op: " << operation
                   << ", range: " << range << ", error: " << FormatError(error);

    // ReportErrorWasSentToTheGuestForReliableDisk(message);
}

ui32 TAlignedDeviceHandler::GetBlockSize() const
{
    return BlockSize;
}

}   // namespace NYdb::NBS::NBlockStore
