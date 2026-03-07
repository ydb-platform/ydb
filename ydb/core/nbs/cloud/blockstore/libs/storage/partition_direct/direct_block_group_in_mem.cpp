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

TVector<TPersistentBufferWriteMeta> TInMemoryDirectBlockGroup::WriteBlocksLocal(
    TExecutorPtr executor,
    ui32 vChunkIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TWriteBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    NThreading::TPromise<TWriteBlocksLocalResponse> promise)
{
    Y_UNUSED(executor);
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);

    // Acquire the sglist guard to access the data
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        // Failed to acquire guard, return error
        TWriteBlocksLocalResponse response;
        response.Error =
            MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        promise.SetValue(std::move(response));
        return {};
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
        return {};
    }

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        TWriteBlocksLocalResponse response;
        response.Error =
            MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        promise.SetValue(std::move(response));
        return {};
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
        promise.SetValue(std::move(response));
        return {};
    }

    promise.SetValue(std::move(response));

    // Return meta compatible with TDirectBlockGroup (3 replicas)
    TVector<TPersistentBufferWriteMeta> meta;
    meta.reserve(3);
    for (ui8 i = 0; i < 3; i++) {
        meta.emplace_back(i, 0);
    }
    return meta;
}

void TInMemoryDirectBlockGroup::SyncWithPersistentBuffer(
    TExecutorPtr executor,
    ui32 vChunkIndex,
    ui8 persistBufferIndex,
    const TVector<TSyncRequest>& syncRequests,
    NWilson::TTraceId traceId)
{
    Y_UNUSED(executor);
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(persistBufferIndex);
    Y_UNUSED(syncRequests);
    Y_UNUSED(traceId);
}

void TInMemoryDirectBlockGroup::ErasePersistentBuffer(
    TExecutorPtr executor,
    std::shared_ptr<TEraseRequestHandler> requestHandler)
{
    Y_UNUSED(executor);
    Y_UNUSED(requestHandler);
}

namespace {

void DoReadFromSectorMap(
    const TIntrusivePtr<NPDisk::TSectorMap>& sectorMap,
    ui32 blockSize,
    const std::shared_ptr<TReadBlocksLocalRequest>& request,
    NThreading::TPromise<TReadBlocksLocalResponse> promise)
{
    // Acquire the sglist guard to access the data
    auto guard = request->Sglist.Acquire();
    if (!guard) {
        // Failed to acquire guard, return error
        TReadBlocksLocalResponse response;
        response.Error =
            MakeError(E_CANCELLED, "Failed to acquire sglist guard");
        promise.SetValue(std::move(response));
        return;
    }

    const auto& sglist = guard.Get();

    // Calculate offset and size
    ui64 startOffset = request->Range.Start * blockSize;
    ui64 totalSize = request->Range.Size() * blockSize;

    if (totalSize != SgListGetSize(sglist)) {
        TReadBlocksLocalResponse response;
        response.Error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Buffer size and sglist size mismatch "
                             << totalSize << " != " << SgListGetSize(sglist));
        promise.SetValue(std::move(response));
        return;
    }

    // Sector map requires 4096-byte alignment
    constexpr ui64 SECTOR_SIZE = 4096;

    // Verify alignment
    if (startOffset % SECTOR_SIZE != 0 || totalSize % SECTOR_SIZE != 0) {
        TReadBlocksLocalResponse response;
        response.Error =
            MakeError(E_ARGUMENT, "Buffer not aligned to sector size");
        promise.SetValue(std::move(response));
        return;
    }

    // Prepare buffer to read into
    TVector<ui8> buffer(totalSize);

    // Read from sector map
    bool readSuccess =
        sectorMap->Read(buffer.data(), buffer.size(), startOffset);

    TReadBlocksLocalResponse response;
    if (!readSuccess) {
        response.Error = MakeError(E_IO, "Failed to read from sector map");
        promise.SetValue(std::move(response));
        return;
    }

    // Copy data from buffer to sglist
    SgListCopy(TBlockDataRef::Create(buffer), sglist);

    promise.SetValue(std::move(response));
}

}   // namespace

void TInMemoryDirectBlockGroup::ReadBlocksLocalFromPersistentBuffer(
    TExecutorPtr executor,
    ui32 vChunkIndex,
    ui8 persistentBufferIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    NThreading::TPromise<TReadBlocksLocalResponse> promise,
    ui64 lsn)
{
    Y_UNUSED(executor);
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(persistentBufferIndex);
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);
    Y_UNUSED(lsn);
    DoReadFromSectorMap(SectorMap, BlockSize, request, std::move(promise));
}

void TInMemoryDirectBlockGroup::ReadBlocksLocalFromDDisk(
    TExecutorPtr executor,
    ui32 vChunkIndex,
    TCallContextPtr callContext,
    std::shared_ptr<TReadBlocksLocalRequest> request,
    NWilson::TTraceId traceId,
    NThreading::TPromise<TReadBlocksLocalResponse> promise)
{
    Y_UNUSED(executor);
    Y_UNUSED(vChunkIndex);
    Y_UNUSED(callContext);
    Y_UNUSED(traceId);
    DoReadFromSectorMap(SectorMap, BlockSize, request, std::move(promise));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
