#include "dirty_map.h"

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

TBlockMeta::TBlockMeta(size_t persistentBufferCount)
    : LsnByPersistentBufferIndex(persistentBufferCount, 0)
    , IsFlushedToDDiskByPersistentBufferIndex(persistentBufferCount, false)
{}

void TBlockMeta::OnWriteCompleted(const TPersistentBufferWriteMeta& writeMeta)
{
    LsnByPersistentBufferIndex[writeMeta.Index] = writeMeta.Lsn;
    IsFlushedToDDiskByPersistentBufferIndex[writeMeta.Index] = false;
}

void TBlockMeta::OnFlushCompleted(size_t persistentBufferIndex, ui64 lsn)
{
    if (LsnByPersistentBufferIndex[persistentBufferIndex] == lsn) {
        LsnByPersistentBufferIndex[persistentBufferIndex] = 0;
        IsFlushedToDDiskByPersistentBufferIndex[persistentBufferIndex] = true;
    }
}

bool TBlockMeta::IsWritten() const
{
    return IsFlushedToDDisk() || LsnByPersistentBufferIndex[0] != 0;
}

bool TBlockMeta::IsFlushedToDDisk() const
{
    return IsFlushedToDDiskByPersistentBufferIndex[0];
}

bool TBlockMeta::ReadyToFlush() const
{
    return !IsFlushedToDDisk();
}

////////////////////////////////////////////////////////////////////////////////

ui64 TDirtyMap::GetLsnByPersistentBufferIndex(
    ui64 blockIndex,
    ui64 persistBufferIndex) const
{
    auto it = BlocksMeta.find(blockIndex);
    if (it == BlocksMeta.end()) {
        return 0;
    }
    return it->second.LsnByPersistentBufferIndex[persistBufferIndex];
}

void TDirtyMap::TryUpdateLsnByPersistentBufferIndex(
    ui64 blockIndex,
    ui64 persistBufferIndex,
    ui64 lsn)
{
    auto it = BlocksMeta.find(blockIndex);
    if (it == BlocksMeta.end()) {
        auto p = BlocksMeta.insert(
            {blockIndex, TBlockMeta(DirtyMapPersistentBuffersCount)});
        Y_ASSERT(p.second);
        it = p.first;
    }

    auto& currentLsn =
        it->second.LsnByPersistentBufferIndex[persistBufferIndex];
    currentLsn = std::max(currentLsn, lsn);
}

bool TDirtyMap::IsBlockWritten(ui64 blockIndex) const
{
    auto it = BlocksMeta.find(blockIndex);
    if (it == BlocksMeta.end()) {
        return false;
    }
    return it->second.IsWritten();
}

bool TDirtyMap::IsBlockFlushedToDDisk(ui64 blockIndex) const
{
    auto it = BlocksMeta.find(blockIndex);
    if (it == BlocksMeta.end()) {
        return false;
    }
    return it->second.IsFlushedToDDisk();
}

void TDirtyMap::OnBlockWriteCompleted(
    ui64 blockIndex,
    const TPersistentBufferWriteMeta& writeMeta)
{
    auto it = BlocksMeta.find(blockIndex);
    if (it == BlocksMeta.end()) {
        auto p = BlocksMeta.insert(
            {blockIndex, TBlockMeta(DirtyMapPersistentBuffersCount)});
        Y_ASSERT(p.second);
        it = p.first;
    }
    it->second.OnWriteCompleted(writeMeta);
}

void TDirtyMap::OnBlockFlushCompleted(
    ui64 blockIndex,
    ui64 persistBufferIndex,
    ui64 lsn)
{
    auto it = BlocksMeta.find(blockIndex);
    if (it == BlocksMeta.end()) {
        auto p = BlocksMeta.insert(
            {blockIndex, TBlockMeta(DirtyMapPersistentBuffersCount)});
        Y_ASSERT(p.second);
        it = p.first;
    }
    it->second.OnFlushCompleted(persistBufferIndex, lsn);
}

void TDirtyMap::VisitEachBlockMeta(
    std::function<void(ui64 blockIndex, const TBlockMeta& blockMeta)> callback)
    const
{
    for (const auto& [blockIndex, blockMeta]: BlocksMeta) {
        callback(blockIndex, blockMeta);
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
