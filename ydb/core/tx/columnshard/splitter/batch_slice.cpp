#include "batch_slice.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap {

class TChunksToSplit {
private:
    YDB_READONLY_DEF(std::vector<std::shared_ptr<IPortionDataChunk>>, Chunks);
    i64 FullSize = 0;
public:
    ui64 GetFullSize() const {
        return FullSize;
    }

    ui32 size() const {
        return Chunks.size();
    }

    void Clear() {
        Chunks.clear();
        FullSize = 0;
    }

    const std::shared_ptr<IPortionDataChunk>& operator[](const ui32 index) const {
        AFL_VERIFY(index < Chunks.size());
        return Chunks[index];
    }

    void AddChunks(const std::vector<std::shared_ptr<IPortionDataChunk>>& chunks) {
        for (auto&& i : chunks) {
            FullSize += i->GetPackedSize();
            Chunks.emplace_back(i);
        }
    }

    void PopFront(const ui32 count) {
        AFL_VERIFY(count <= Chunks.size());
        for (ui32 i = 0; i < count; ++i) {
            FullSize -= Chunks[i]->GetPackedSize();
        }
        AFL_VERIFY(FullSize >= 0);
        Chunks.erase(Chunks.begin(), Chunks.begin() + count);
    }

    void Exchange(const ui32 index, std::vector<std::shared_ptr<IPortionDataChunk>>&& newChunks) {
        AFL_VERIFY(index < Chunks.size());
        FullSize -= Chunks[index]->GetPackedSize();
        AFL_VERIFY(FullSize >= 0);
        for (auto&& i : newChunks) {
            FullSize += i->GetPackedSize();
        }
        Chunks.erase(Chunks.begin() + index);
        Chunks.insert(Chunks.begin() + index, newChunks.begin(), newChunks.end());
    }

    bool IsEmpty() {
        return Chunks.empty();
    }
};

bool TGeneralSerializedSlice::GroupBlobsImpl(const NSplitter::TGroupFeatures& features, std::vector<TSplittedBlob>& blobs) {
    TChunksToSplit chunksInProgress;
    std::sort(Data.begin(), Data.end());
    for (auto&& i : Data) {
        if (!features.Contains(i.GetEntityId())) {
            continue;
        }
        chunksInProgress.AddChunks(i.GetChunks());
    }
    InternalSplitsCount = 0;
    std::vector<TSplittedBlob> result;
    Y_ABORT_UNLESS(features.GetSplitSettings().GetMaxBlobSize() >= 2 * features.GetSplitSettings().GetMinBlobSize());
    while (!chunksInProgress.IsEmpty()) {
        bool hasNoSplitChanges = true;
        while (hasNoSplitChanges) {
            if (chunksInProgress.GetFullSize() < (ui64)features.GetSplitSettings().GetMaxBlobSize()) {
                result.emplace_back(TSplittedBlob(features.GetName()));
                for (auto&& i : chunksInProgress.GetChunks()) {
                    result.back().Take(i);
                }
                chunksInProgress.Clear();
                break;
            }
            hasNoSplitChanges = false;
            i64 partSize = 0;
            for (ui32 i = 0; i < chunksInProgress.size(); ++i) {
                const i64 nextPartSize = partSize + chunksInProgress[i]->GetPackedSize();
                const i64 nextOtherSize = chunksInProgress.GetFullSize() - nextPartSize;
                const i64 otherSize = chunksInProgress.GetFullSize() - partSize;
                if (nextPartSize >= features.GetSplitSettings().GetMaxBlobSize() || nextOtherSize < features.GetSplitSettings().GetMinBlobSize()) {
                    Y_ABORT_UNLESS(otherSize >= features.GetSplitSettings().GetMinBlobSize());
                    Y_ABORT_UNLESS(partSize < features.GetSplitSettings().GetMaxBlobSize());
                    if (partSize >= features.GetSplitSettings().GetMinBlobSize()) {
                        result.emplace_back(TSplittedBlob(features.GetName()));
                        for (ui32 chunk = 0; chunk < i; ++chunk) {
                            result.back().Take(chunksInProgress[chunk]);
                        }
                        Counters->BySizeSplitter.OnCorrectSerialized(result.back().GetSize());
                        chunksInProgress.PopFront(i);
                        hasNoSplitChanges = true;
                    } else {
                        Y_ABORT_UNLESS((i64)chunksInProgress[i]->GetPackedSize() > features.GetSplitSettings().GetMinBlobSize() - partSize);
                        Y_ABORT_UNLESS(otherSize - (features.GetSplitSettings().GetMinBlobSize() - partSize) >= features.GetSplitSettings().GetMinBlobSize());

                        std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
                        const bool splittable = chunksInProgress[i]->IsSplittable();
                        if (splittable) {
                            Counters->BySizeSplitter.OnTrashSerialized(chunksInProgress[i]->GetPackedSize());
                            const std::vector<ui64> sizes = {(ui64)(features.GetSplitSettings().GetMinBlobSize() - partSize)};
                            newChunks = chunksInProgress[i]->InternalSplit(Schema->GetColumnSaver(chunksInProgress[i]->GetEntityId()), Counters, sizes);
                            ++InternalSplitsCount;
                            chunksInProgress.Exchange(i, std::move(newChunks));
                        }

                        TSplittedBlob newBlob(features.GetName());
                        for (ui32 chunk = 0; chunk <= i; ++chunk) {
                            newBlob.Take(chunksInProgress[chunk]);
                        }
                        AFL_VERIFY(splittable || newBlob.GetSize() < features.GetSplitSettings().GetMaxBlobSize())("splittable", splittable)("blob_size", newBlob.GetSize())("max", features.GetSplitSettings().GetMaxBlobSize());
                        if (newBlob.GetSize() < features.GetSplitSettings().GetMaxBlobSize()) {
                            chunksInProgress.PopFront(i + 1);
                            result.emplace_back(std::move(newBlob));
                            Counters->BySizeSplitter.OnCorrectSerialized(result.back().GetSize());
                        }
                    }
                    break;
                }
                partSize = nextPartSize;
            }
        }
    }
    std::set<ui32> columnIds;
    std::optional<ui32> lastColumnId;
    ui32 currentChunkIdx = 0;
    for (auto&& i : result) {
        for (auto&& c : i.GetChunks()) {
            Y_ABORT_UNLESS(c->GetEntityId());
            if (!lastColumnId || *lastColumnId != c->GetEntityId()) {
                Y_ABORT_UNLESS(columnIds.emplace(c->GetEntityId()).second);
                lastColumnId = c->GetEntityId();
                currentChunkIdx = 0;
            }
            c->SetChunkIdx(currentChunkIdx++);
        }
    }
    std::swap(blobs, result);
    return true;
}

TGeneralSerializedSlice::TGeneralSerializedSlice(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data,
    NArrow::NSplitter::ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters)
    : Schema(schema)
    , Counters(counters) {
    std::optional<ui32> recordsCount;
    for (auto&& [entityId, chunks] : data) {
        TSplittedEntity entity(entityId);
        entity.SetChunks(chunks);
        if (!!entity.GetRecordsCount()) {
            if (!recordsCount) {
                recordsCount = entity.GetRecordsCount();
            } else {
                AFL_VERIFY(*recordsCount == entity.GetRecordsCount())("records_count", *recordsCount)("column", entity.GetRecordsCount());
            }
        }
        Size += entity.GetSize();
        Data.emplace_back(std::move(entity));
    }
    Y_ABORT_UNLESS(recordsCount);
    RecordsCount = *recordsCount;
}

TGeneralSerializedSlice::TGeneralSerializedSlice(
    const ui32 recordsCount, NArrow::NSplitter::ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters)
    : RecordsCount(recordsCount)
    , Schema(schema)
    , Counters(counters)
{
}

void TGeneralSerializedSlice::MergeSlice(TGeneralSerializedSlice&& slice) {
    Y_ABORT_UNLESS(Data.size() == slice.Data.size());
    RecordsCount += slice.GetRecordsCount();
    for (ui32 i = 0; i < Data.size(); ++i) {
        Size += slice.Data[i].GetSize();
        Data[i].Merge(std::move(slice.Data[i]));
    }
}

bool TGeneralSerializedSlice::GroupBlobs(std::vector<TSplittedBlob>& blobs, const NSplitter::TEntityGroups& groups) {
    if (groups.IsEmpty()) {
        return GroupBlobsImpl(groups.GetDefaultGroupFeatures(), blobs);
    } else {
        std::vector<TSplittedBlob> result;
        for (auto&& i : groups) {
            std::vector<TSplittedBlob> blobsLocal;
            if (!GroupBlobsImpl(i.second, blobsLocal)) {
                return false;
            }
            result.insert(result.end(), blobsLocal.begin(), blobsLocal.end());
        }
        std::swap(result, blobs);
        return true;
    }
}

}
