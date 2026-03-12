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
    const std::shared_ptr<TReadBlocksLocalRequest>& request,
    NThreading::TPromise<TDBGReadBlocksResponse> promise)
{
    if (auto guard = request->Sglist.Acquire()) {
        // Acquire the sglist guard to access the data
        const auto& sglist = guard.Get();

        // Calculate offset and size
        ui64 startOffset = request->Range.Start * blockSize;
        ui64 totalSize = request->Range.Size() * blockSize;

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

NThreading::TFuture<void> TInMemoryDirectBlockGroup::EstablishConnections(
    TExecutorPtr executor,
    NWilson::TTraceId traceId,
    ui32 vChunkIndex)
{
    Y_UNUSED(executor);
    Y_UNUSED(traceId);
    Y_UNUSED(vChunkIndex);

    return NThreading::MakeFuture();
}

NThreading::TFuture<TDBGWriteBlocksResponse>
TInMemoryDirectBlockGroup::WriteBlocksLocal(
    ui32 vChunkIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);

    // Acquire the sglist guard to access the data
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        // Failed to acquire guard, return error
        auto error = MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
            {.Error = std::move(error)});
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = request->Range.Start * BlockSize;
    ui64 totalSize = request->Range.Size() * BlockSize;

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
    // Return meta compatible with TDirectBlockGroup (3 replicas)
    TVector<TPersistentBufferWriteMeta> meta;
    meta.reserve(3);
    for (ui8 i = 0; i < 3; i++) {
        meta.emplace_back(i, 0);
    }
    return NThreading::MakeFuture<TDBGWriteBlocksResponse>(
        {.Meta = std::move(meta), .Error = MakeError(S_OK)});
}

NThreading::TFuture<TDBGSyncBlocksResponse>
TInMemoryDirectBlockGroup::SyncWithPersistentBuffer(
    ui32 vChunkIndex,
    ui8 persistBufferIndex,
    const TVector<TSyncRequest>& syncRequests,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(persistBufferIndex);
    Y_UNUSED(syncRequests);
    Y_UNUSED(traceId);

    return NThreading::MakeFuture<TDBGSyncBlocksResponse>(
        {.Error = MakeError(S_OK)});
}

NThreading::TFuture<TDBGReadBlocksResponse>
TInMemoryDirectBlockGroup::ReadBlocksLocalFromPersistentBuffer(
    ui32 vChunkIndex,
    ui8 persistentBufferIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    ui64 lsn)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(persistentBufferIndex);
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);
    Y_UNUSED(lsn);

    auto promise = NThreading::NewPromise<TDBGReadBlocksResponse>();
    auto future = promise.GetFuture();

    DoReadFromSectorMap(SectorMap, BlockSize, request, std::move(promise));
    return future;
}

NThreading::TFuture<TDBGReadBlocksResponse>
TInMemoryDirectBlockGroup::ReadBlocksLocalFromDDisk(
    ui32 vChunkIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);

    auto promise = NThreading::NewPromise<TDBGReadBlocksResponse>();
    auto future = promise.GetFuture();

    DoReadFromSectorMap(SectorMap, BlockSize, request, std::move(promise));
    return future;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
