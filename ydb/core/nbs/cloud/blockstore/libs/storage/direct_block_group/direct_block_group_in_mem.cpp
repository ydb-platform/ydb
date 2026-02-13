#include "direct_block_group_in_mem.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NKikimr;
using namespace NYdb::NBS;

TInMemoryDirectBlockGroup::TInMemoryDirectBlockGroup(
    ui64 tabletId, ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds, ui32 blockSize,
    ui64 blocksCount)
    : BlockSize(blockSize)
{
    Y_UNUSED(tabletId);
    Y_UNUSED(generation);
    Y_UNUSED(ddisksIds);
    Y_UNUSED(persistentBufferDDiskIds);

    // Calculate the device size based on blocks count and block size
    ui64 deviceSize = blocksCount * blockSize;

    // Create an in-memory sector map
    SectorMap = MakeIntrusive<NPDisk::TSectorMap>(deviceSize,
                                                  NPDisk::NSectorMap::DM_NONE);
}

void TInMemoryDirectBlockGroup::EstablishConnections()
{}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TWriteBlocksLocalResponse>
TInMemoryDirectBlockGroup::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);

    auto promise = NThreading::NewPromise<TWriteBlocksLocalResponse>();

    // Acquire the sglist guard to access the data
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        // Failed to acquire guard, return error
        TWriteBlocksLocalResponse response;
        response.Error =
            MakeError(E_REJECTED, "Failed to acquire sglist guard");
        promise.SetValue(response);
        return promise.GetFuture();
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = request->Range.Start * BlockSize;
    ui64 totalSize = request->Range.Size() * BlockSize;

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        TWriteBlocksLocalResponse response;
        response.Error =
            MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        promise.SetValue(response);
        return promise.GetFuture();
    }

    // Prepare buffer to write
    TVector<ui8> buffer;
    buffer.reserve(totalSize);

    // Copy data from sglist to buffer
    for (const auto& blockDataRef: sglist) {
        if (blockDataRef.Data() && blockDataRef.Size() > 0) {
            const ui8* data = reinterpret_cast<const ui8*>(blockDataRef.Data());
            buffer.insert(buffer.end(), data, data + blockDataRef.Size());
        } else {
            // Zero block
            buffer.insert(buffer.end(), blockDataRef.Size(), 0);
        }
    }

    // Write to sector map
    bool writeSuccess =
        SectorMap->Write(buffer.data(), buffer.size(), startOffset);

    TWriteBlocksLocalResponse response;
    if (!writeSuccess) {
        response.Error = MakeError(E_IO, "Failed to write to sector map");
    }

    if (WriteBlocksReplyCallback) {
        WriteBlocksReplyCallback(true);
    }

    promise.SetValue(response);
    return promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse>
TInMemoryDirectBlockGroup::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request, NWilson::TTraceId traceId)
{
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);

    auto promise = NThreading::NewPromise<TReadBlocksLocalResponse>();

    // Acquire the sglist guard to access the data
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        // Failed to acquire guard, return error
        TReadBlocksLocalResponse response;
        response.Error =
            MakeError(E_REJECTED, "Failed to acquire sglist guard");
        promise.SetValue(response);
        return promise.GetFuture();
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = request->Range.Start * BlockSize;
    ui64 totalSize = request->Range.Size() * BlockSize;

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        TReadBlocksLocalResponse response;
        response.Error =
            MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        promise.SetValue(response);
        return promise.GetFuture();
    }

    // Prepare buffer to read into
    TVector<ui8> buffer(totalSize);

    // Read from sector map
    bool readSuccess =
        SectorMap->Read(buffer.data(), buffer.size(), startOffset);

    TReadBlocksLocalResponse response;
    if (!readSuccess) {
        response.Error = MakeError(E_IO, "Failed to read from sector map");
        promise.SetValue(response);
        return promise.GetFuture();
    }

    // Copy data from buffer to sglist
    size_t bufferOffset = 0;
    for (const auto& blockDataRef: sglist) {
        if (blockDataRef.Data() && blockDataRef.Size() > 0) {
            ui8* dest = const_cast<ui8*>(
                reinterpret_cast<const ui8*>(blockDataRef.Data()));
            size_t copySize =
                std::min<size_t>(blockDataRef.Size(), totalSize - bufferOffset);
            memcpy(dest, buffer.data() + bufferOffset, copySize);
            bufferOffset += copySize;
        } else {
            // Skip zero blocks
            bufferOffset += blockDataRef.Size();
        }

        if (bufferOffset >= totalSize) {
            break;
        }
    }

    if (ReadBlocksReplyCallback) {
        ReadBlocksReplyCallback(true);
    }

    promise.SetValue(response);
    return promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
