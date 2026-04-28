#include "direct_block_group_in_mem.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

using namespace NKikimr;
using namespace NYdb::NBS;

namespace {

void DoReadFromSectorMap(
    const TIntrusivePtr<NPDisk::TSectorMap>& sectorMap,
    ui32 blockSize,
    const TGuardedSgList& sgList,
    TBlockRange64 range,
    NThreading::TPromise<TDBGReadBlocksResponse> promise)
{
    if (auto guard = sgList.Acquire()) {
        // Acquire the sglist guard to access the data
        const auto& sglist = guard.Get();

        // Calculate offset and size
        ui64 startOffset = range.Start * blockSize;
        ui64 totalSize = range.Size() * blockSize;

        if (totalSize != SgListGetSize(sglist)) {
            auto error = MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Buffer size and sglist size mismatch " << totalSize
                    << " != " << SgListGetSize(sglist));
            promise.SetValue({.Error = std::move(error)});
            return;
        }

        // Sector map requires 4096-byte alignment
        constexpr ui64 SECTOR_SIZE = 4096;

        // Verify alignment
        if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
            auto error =
                MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
            promise.SetValue({.Error = std::move(error)});
            return;
        }

        // Prepare buffer to read into
        TVector<ui8> buffer(totalSize);

        // Read from sector map
        bool readSuccess =
            sectorMap->Read(buffer.data(), buffer.size(), startOffset);

        if (!readSuccess) {
            auto error = MakeError(E_IO, "Failed to read from sector map");
            promise.SetValue({.Error = std::move(error)});
            return;
        }

        // Copy data from buffer to sglist
        SgListCopy(TBlockDataRef::Create(buffer), sglist);
    } else {
        // Failed to acquire guard, return error
        auto error = MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        promise.SetValue({.Error = std::move(error)});
        return;
    }

    promise.SetValue({.Error = MakeError(S_OK)});
}

}   // namespace

TInMemoryDirectBlockGroup::TInMemoryDirectBlockGroup(
    ui64 tabletId,
    ui32 generation,
    TVector<NBsController::TDDiskId> ddisksIds,
    TVector<NBsController::TDDiskId> persistentBufferDDiskIds,
    ui32 blockSize,
    ui64 blocksCount)
    : TabletId(tabletId)
    , BlockSize(blockSize)
{
    Y_UNUSED(generation);
    Y_UNUSED(ddisksIds);
    Y_UNUSED(persistentBufferDDiskIds);
    Y_UNUSED(TabletId);

    // Calculate the device size based on blocks count and block size
    ui64 deviceSize = blocksCount * blockSize;

    // Create an in-memory sector map
    SectorMap = MakeIntrusive<NPDisk::TSectorMap>(
        deviceSize,
        NPDisk::NSectorMap::DM_NONE);
}

void TInMemoryDirectBlockGroup::EstablishConnections()
{}

NThreading::TFuture<TDBGWriteBlocksResponse>
TInMemoryDirectBlockGroup::WriteBlocksToPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& guardedSglist,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(hostIndex);
    Y_UNUSED(lsn);
    Y_UNUSED(traceId);

    // Acquire the sglist guard to access the data
    auto guard = guardedSglist.Acquire();
    if (!guard) {
        // Failed to acquire guard, return error
        auto error = MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
            {.Error = std::move(error)});
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = range.Start * BlockSize;
    ui64 totalSize = range.Size() * BlockSize;

    if (totalSize != SgListGetSize(sglist)) {
        auto error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Buffer size and sglist size mismatch "
                             << totalSize << " != " << SgListGetSize(sglist));
        return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
            {.Error = std::move(error)});
    }

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        auto error = MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
            {.Error = std::move(error)});
    }

    // Prepare buffer to write
    TVector<ui8> buffer;
    buffer.resize(totalSize);

    // Copy data from sglist to buffer
    SgListCopy(sglist, TBlockDataRef::Create(buffer));

    // Write to sector map
    bool writeSuccess =
        SectorMap->Write(buffer.data(), buffer.size(), startOffset);

    if (!writeSuccess) {
        auto error = MakeError(E_IO, "Failed to write to sector map");
        return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
            {.Error = std::move(error)});
    }

    return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
        {.Error = MakeError(S_OK)});
}

NThreading::TFuture<TDBGFlushResponse>
TInMemoryDirectBlockGroup::SyncWithPBuffer(
    ui32 vChunkIndex,
    ui8 pbufferHostIndex,
    ui8 ddiskHostIndex,
    const TVector<TPBufferSegment>& segments,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(pbufferHostIndex);
    Y_UNUSED(ddiskHostIndex);
    Y_UNUSED(segments);
    Y_UNUSED(traceId);

    TDBGFlushResponse response;
    for (const auto& _: segments) {
        response.Errors.push_back(MakeError(S_OK));
    }

    return NThreading::MakeFuture<TDBGFlushResponse>(std::move(response));
}

NThreading::TFuture<TDBGReadBlocksResponse>
TInMemoryDirectBlockGroup::ReadBlocksFromPBuffer(
    ui32 vChunkIndex,
    ui8 hostIndex,
    ui64 lsn,
    TBlockRange64 range,
    const TGuardedSgList& sglist,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(hostIndex);
    Y_UNUSED(traceId);
    Y_UNUSED(lsn);

    auto promise = NThreading::NewPromise<TDBGReadBlocksResponse>();
    auto future = promise.GetFuture();

    DoReadFromSectorMap(
        SectorMap,
        BlockSize,
        sglist,
        range,
        std::move(promise));
    return future;
}

NThreading::TFuture<TDBGReadBlocksResponse>
TInMemoryDirectBlockGroup::ReadBlocksFromDDisk(
    ui32 vChunkIndex,
    ui8 hostIndex,
    TBlockRange64 range,
    const TGuardedSgList& sglist,
    const NWilson::TTraceId& traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(hostIndex), Y_UNUSED(traceId);

    auto promise = NThreading::NewPromise<TDBGReadBlocksResponse>();
    auto future = promise.GetFuture();

    DoReadFromSectorMap(
        SectorMap,
        BlockSize,
        sglist,
        range,
        std::move(promise));
    return future;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
