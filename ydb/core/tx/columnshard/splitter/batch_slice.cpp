#include "batch_slice.h"
#include "simple.h"
#include <ydb/library/accessor/validator.h>

namespace NKikimr::NOlap {

bool TGeneralSerializedSlice::GroupBlobsImpl(const TString& currentGroupName, const std::set<ui32>& entityIds, std::vector<TSplittedBlob>& blobs) {
    std::vector<std::shared_ptr<IPortionDataChunk>> chunksInProgress;
    std::sort(Data.begin(), Data.end());
    for (auto&& i : Data) {
        if (!entityIds.empty() && !entityIds.contains(i.GetEntityId())) {
            continue;
        }
        for (auto&& p : i.GetChunks()) {
            chunksInProgress.emplace_back(p);
        }
    }
    AFL_VERIFY(chunksInProgress.size());
    std::vector<TSplittedBlob> result;
    Y_ABORT_UNLESS(Settings.GetMaxBlobSize() >= 2 * Settings.GetMinBlobSize());
    while (chunksInProgress.size()) {
        i64 fullSize = 0;
        for (auto&& i : chunksInProgress) {
            fullSize += i->GetPackedSize();
        }
        if (fullSize < Settings.GetMaxBlobSize()) {
            result.emplace_back(TSplittedBlob(currentGroupName));
            for (auto&& i : chunksInProgress) {
                result.back().Take(i);
            }
            chunksInProgress.clear();
            break;
        }
        bool hasNoSplitChanges = true;
        while (hasNoSplitChanges) {
            hasNoSplitChanges = false;
            i64 partSize = 0;
            for (ui32 i = 0; i < chunksInProgress.size(); ++i) {
                const i64 nextPartSize = partSize + chunksInProgress[i]->GetPackedSize();
                const i64 nextOtherSize = fullSize - nextPartSize;
                const i64 otherSize = fullSize - partSize;
                if (nextPartSize >= Settings.GetMaxBlobSize() || nextOtherSize < Settings.GetMinBlobSize()) {
                    Y_ABORT_UNLESS(otherSize >= Settings.GetMinBlobSize());
                    Y_ABORT_UNLESS(partSize < Settings.GetMaxBlobSize());
                    if (partSize >= Settings.GetMinBlobSize()) {
                        result.emplace_back(TSplittedBlob(currentGroupName));
                        for (ui32 chunk = 0; chunk < i; ++chunk) {
                            result.back().Take(chunksInProgress[chunk]);
                        }
                        Counters->BySizeSplitter.OnCorrectSerialized(result.back().GetSize());
                        chunksInProgress.erase(chunksInProgress.begin(), chunksInProgress.begin() + i);
                        hasNoSplitChanges = true;
                    } else {
                        Y_ABORT_UNLESS((i64)chunksInProgress[i]->GetPackedSize() > Settings.GetMinBlobSize() - partSize);
                        Y_ABORT_UNLESS(otherSize - (Settings.GetMinBlobSize() - partSize) >= Settings.GetMinBlobSize());

                        std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
                        const bool splittable = chunksInProgress[i]->IsSplittable();
                        if (splittable) {
                            Counters->BySizeSplitter.OnTrashSerialized(chunksInProgress[i]->GetPackedSize());
                            const std::vector<ui64> sizes = {(ui64)(Settings.GetMinBlobSize() - partSize)};
                            newChunks = chunksInProgress[i]->InternalSplit(Schema->GetColumnSaver(chunksInProgress[i]->GetEntityId()), Counters, sizes);
                            chunksInProgress.erase(chunksInProgress.begin() + i);
                            chunksInProgress.insert(chunksInProgress.begin() + i, newChunks.begin(), newChunks.end());
                        }

                        TSplittedBlob newBlob(currentGroupName);
                        for (ui32 chunk = 0; chunk <= i; ++chunk) {
                            newBlob.Take(chunksInProgress[chunk]);
                        }
                        AFL_VERIFY(splittable || newBlob.GetSize() < Settings.GetMaxBlobSize())("splittable", splittable)("blob_size", newBlob.GetSize())("max", Settings.GetMaxBlobSize());
                        if (newBlob.GetSize() < Settings.GetMaxBlobSize()) {
                            chunksInProgress.erase(chunksInProgress.begin(), chunksInProgress.begin() + i + 1);
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

TGeneralSerializedSlice::TGeneralSerializedSlice(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters,
                                                 const TSplitSettings& settings)
    : Schema(schema)
    , Counters(counters)
    , Settings(settings) {
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

TGeneralSerializedSlice::TGeneralSerializedSlice(const ui32 recordsCount, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings)
    : RecordsCount(recordsCount)
    , Schema(schema)
    , Counters(counters)
    , Settings(settings)
{
}

TBatchSerializedSlice::TBatchSerializedSlice(const std::shared_ptr<arrow::RecordBatch>& batch, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings)
    : TBase(TValidator::CheckNotNull(batch)->num_rows(), schema, counters, settings)
    , Batch(batch)
{
    Y_ABORT_UNLESS(batch);
    Data.reserve(batch->num_columns());
    for (auto&& i : batch->schema()->fields()) {
        TSplittedEntity c(schema->GetColumnId(i->name()));
        Data.emplace_back(std::move(c));
    }

    ui32 idx = 0;
    for (auto&& i : batch->columns()) {
        auto& c = Data[idx];
        auto columnSaver = schema->GetColumnSaver(c.GetEntityId());
        auto stats = schema->GetColumnSerializationStats(c.GetEntityId());
        TSimpleSplitter splitter(columnSaver, Counters);
        splitter.SetStats(stats);
        std::vector<std::shared_ptr<IPortionDataChunk>> chunks;
        for (auto&& i : splitter.Split(i, Schema->GetField(c.GetEntityId()), Settings.GetMaxBlobSize())) {
            chunks.emplace_back(std::make_shared<TSplittedColumnChunk>(c.GetEntityId(), i, Schema));
        }
        c.SetChunks(chunks);
        Size += c.GetSize();
        ++idx;
    }
}

void TGeneralSerializedSlice::MergeSlice(TGeneralSerializedSlice&& slice) {
    Y_ABORT_UNLESS(Data.size() == slice.Data.size());
    RecordsCount += slice.GetRecordsCount();
    for (ui32 i = 0; i < Data.size(); ++i) {
        Size += slice.Data[i].GetSize();
        Data[i].Merge(std::move(slice.Data[i]));
    }
}

}
