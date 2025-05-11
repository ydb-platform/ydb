#include "batch_slice.h"

#include <ydb/library/accessor/validator.h>
#include <ydb/library/formats/arrow/splitter/similar_packer.h>

#include <util/string/join.h>

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
    const ui64 maxSizeLimitReal = std::max<ui64>(20000, features.GetSplitSettings().GetMaxBlobSize());
    for (auto&& i : Data) {
        if (!features.Contains(i.GetEntityId())) {
            continue;
        }
        std::vector<std::shared_ptr<IPortionDataChunk>> chunks;
        for (auto&& check : i.GetChunks()) {
            if (check->GetPackedSize() >= (ui64)features.GetSplitSettings().GetMaxBlobSize()) {
                Counters->BySizeSplitter.OnTrashSerialized(check->GetPackedSize());
                AFL_VERIFY(check->IsSplittable())("entity_id", check->GetEntityId())("size", check->GetPackedSize());
                const ui32 countSplit = check->GetPackedSize() / features.GetSplitSettings().GetMaxBlobSize() + 1;
                const ui32 sizeSplit = check->GetPackedSize() / countSplit;
                const std::vector<i64> sizes = NArrow::NSplitter::TSimilarPacker::SplitWithExpected(check->GetPackedSize(), sizeSplit);
                const std::vector<ui64> sizesUI64(sizes.begin(), sizes.end());
                auto chunksImpl = check->InternalSplit(Schema->GetColumnSaver(check->GetEntityId()), Counters, sizesUI64);
                for (auto&& checkImpl : chunksImpl) {
                    AFL_VERIFY(checkImpl->GetPackedSize() < maxSizeLimitReal)("entity_id", checkImpl->GetEntityId())(
                        "size", checkImpl->GetPackedSize())("limit", features.GetSplitSettings().GetMaxBlobSize())("sizes", JoinSeq(",", sizes))(
                        "p_size", check->GetPackedSize());
                }
                chunks.insert(chunks.end(), chunksImpl.begin(), chunksImpl.end());
                ++InternalSplitsCount;
            } else {
                Counters->BySizeSplitter.OnCorrectSerialized(check->GetPackedSize());
                chunks.emplace_back(check);
            }
        }
        for (ui32 idx = 0; idx < chunks.size(); ++idx) {
            chunks[idx]->SetChunkIdx(idx);
        }
        chunksInProgress.AddChunks(std::move(chunks));
    }
    std::vector<TSplittedBlob> result;
    result.emplace_back(TSplittedBlob(features.GetName()));
    TSplittedBlob* currentBlob = &result.back();
    for (ui32 i = 0; i < chunksInProgress.size(); ++i) {
        if (currentBlob->GetSize() + chunksInProgress[i]->GetPackedSize() >= maxSizeLimitReal) {
            AFL_VERIFY(currentBlob->GetSize());
            result.emplace_back(TSplittedBlob(features.GetName()));
            currentBlob = &result.back();
        }
        currentBlob->Take(chunksInProgress[i]);
    }
    if (result.size() && result.back().GetSize() == 0) {
        result.pop_back();
    }
    AFL_VERIFY(result.size() || chunksInProgress.IsEmpty());
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
                AFL_VERIFY(*recordsCount == entity.GetRecordsCount())("records_count", *recordsCount)("column", entity.GetRecordsCount())(
                                              "chunks", chunks.size());
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
    , Counters(counters) {
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

}   // namespace NKikimr::NOlap
