#include "direct_block_group_in_mem.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

using namespace NKikimr;
using namespace NYdb::NBS;

TInMemoryDirectBlockGroup::TInMemoryDirectBlockGroup(
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
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
    SectorMap = MakeIntrusive<NPDisk::TSectorMap>(
        deviceSize,
        NPDisk::NSectorMap::DM_NONE);
}

void TInMemoryDirectBlockGroup::EstablishConnections()
{}


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
            MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        promise.SetValue(std::move(response));
        return promise.GetFuture();
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = request->Range.Start * BlockSize;
    ui64 totalSize = request->Range.Size() * BlockSize;

    if (totalSize != SgListGetSize(sglist)) {
        TWriteBlocksLocalResponse response;
        response.Error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Buffer size and sglist size mismatch "
                             << totalSize << " != " << SgListGetSize(sglist));
        promise.SetValue(std::move(response));
        return promise.GetFuture();
    }

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        TWriteBlocksLocalResponse response;
        response.Error =
            MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        promise.SetValue(std::move(response));
        return promise.GetFuture();
    }

    // Prepare buffer to write
    TVector<ui8> buffer;
    buffer.resize(totalSize);

    // Copy data from sglist to buffer
    SgListCopy(sglist, TBlockDataRef::Create(buffer));

    // Write to sector map
    bool writeSuccess =
        SectorMap->Write(buffer.data(), buffer.size(), startOffset);

    TWriteBlocksLocalResponse response;
    if (!writeSuccess) {
        response.Error = MakeError(E_IO, "Failed to write to sector map");
    }

    promise.SetValue(std::move(response));
    return promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

NThreading::TFuture<TReadBlocksLocalResponse>
TInMemoryDirectBlockGroup::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
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
            MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        promise.SetValue(std::move(response));
        return promise.GetFuture();
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = request->Range.Start * BlockSize;
    ui64 totalSize = request->Range.Size() * BlockSize;

    if (totalSize != SgListGetSize(sglist)) {
        TReadBlocksLocalResponse response;
        response.Error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Buffer size and sglist size mismatch "
                             << totalSize << " != " << SgListGetSize(sglist));
        promise.SetValue(std::move(response));
        return promise.GetFuture();
    }

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        TReadBlocksLocalResponse response;
        response.Error =
            MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        promise.SetValue(std::move(response));
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
        promise.SetValue(std::move(response));
        return promise.GetFuture();
    }

    // Copy data from buffer to sglist
    SgListCopy(TBlockDataRef::Create(buffer), sglist);

    promise.SetValue(std::move(response));
    return promise.GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
