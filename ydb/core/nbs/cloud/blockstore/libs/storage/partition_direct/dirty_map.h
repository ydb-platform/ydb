#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/service/request.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/request.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t DirtyMapPersistentBuffersCount = 5;

////////////////////////////////////////////////////////////////////////////////

struct TBlockMeta
{
    TVector<ui64> LsnByPersistentBufferIndex;
    TVector<bool> IsFlushedToDDiskByPersistentBufferIndex;

    explicit TBlockMeta(size_t persistentBufferCount);

    void OnWriteCompleted(const TPersistentBufferWriteMeta& writeMeta);

    void OnFlushCompleted(size_t persistentBufferIndex, ui64 lsn);

    [[nodiscard]] bool IsWritten() const;
    [[nodiscard]] bool IsFlushedToDDisk() const;
    [[nodiscard]] bool ReadyToFlush() const;
};

////////////////////////////////////////////////////////////////////////////////

class TDirtyMap
{
public:
    ui64 GetLsnByPersistentBufferIndex(
        ui64 blockIndex,
        ui64 persistBufferIndex) const;

    void TryUpdateLsnByPersistentBufferIndex(
        ui64 blockIndex,
        ui64 persistBufferIndex,
        ui64 lsn);

    [[nodiscard]] bool IsBlockWritten(ui64 blockIndex) const;

    [[nodiscard]] bool IsBlockFlushedToDDisk(ui64 blockIndex) const;

    void OnBlockWriteCompleted(
        ui64 blockIndex,
        const TPersistentBufferWriteMeta& writeMeta);

    void
    OnBlockFlushCompleted(ui64 blockIndex, ui64 persistBufferIndex, ui64 lsn);

    void VisitEachBlockMeta(
        std::function<void(ui64 blockIndex, const TBlockMeta& blockMeta)>
            callback) const;

private:
    THashMap<ui64, TBlockMeta> BlocksMeta;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
