#pragma once

#include "blocks_info.h"
#include "device_handler.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// The TAlignedDeviceHandler can only process requests that are aligned. If the
// size of a request exceeds the maximum size that the underlying layer can
// handle, the TAlignedDeviceHandler will break the request into smaller parts
// and execute them separately. If a request contains unaligned data, the
// E_ARGUMENT error is returned.
class TAlignedDeviceHandler final
    : public IDeviceHandler
    , public std::enable_shared_from_this<TAlignedDeviceHandler>
{
private:
    const IStoragePtr Storage;
    const TString DiskId;
    const TString ClientId;
    const ui32 BlockSize;
    const ui32 MaxBlockCount;
    const ui32 MaxBlockCountForZeroBlocksRequest;

public:
    TAlignedDeviceHandler(TDeviceHandlerParams params, ui32 maxSubRequestSize);

    // implements IDeviceHandler
    NThreading::TFuture<TReadBlocksLocalResponse> Read(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList,
        const TString& checkpointId) override;

    NThreading::TFuture<TWriteBlocksLocalResponse> Write(
        TCallContextPtr ctx,
        ui64 from,
        ui64 length,
        TGuardedSgList sgList) override;

    NThreading::TFuture<TZeroBlocksLocalResponse>
    Zero(TCallContextPtr ctx, ui64 from, ui64 length) override;

    // Performs a read. It can only be called for aligned data.
    NThreading::TFuture<TReadBlocksLocalResponse> ExecuteReadRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList,
        TString checkpointId);

    // Performs a write. It can only be called for aligned data.
    NThreading::TFuture<TWriteBlocksLocalResponse> ExecuteWriteRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo,
        TGuardedSgList sgList);

    // Performs a zeroes. It can only be called for aligned data.
    NThreading::TFuture<TZeroBlocksLocalResponse> ExecuteZeroRequest(
        TCallContextPtr ctx,
        TBlocksInfo blocksInfo);

    ui32 GetBlockSize() const;

private:
    void ReportCriticalError(
        const NProto::TError& error,
        const TString& operation,
        TBlockRange64 range);
};

////////////////////////////////////////////////////////////////////////////////

// Normalizes the SgList in guardedSgList. If the total size of the buffers does
// not match the request size, an error is returned. If it is not possible to
// normalize the number of buffers so that they correspond to the number of
// requested blocks and the size of each buffer is equal to the specified block
// size, the SgListAligned flag is set in the blocksInfo structure, but no error
// is returned. This indicates that the request is valid, but not aligned.
NProto::TError TryToNormalize(
    TGuardedSgList& guardedSgList,
    TBlocksInfo& blocksInfo);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
