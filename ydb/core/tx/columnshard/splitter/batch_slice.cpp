#include "batch_slice.h"
#include "simple.h"

namespace NKikimr::NOlap {

bool TGeneralSerializedSlice::GroupBlobs(std::vector<TSplittedBlob>& blobs) {
    std::vector<IPortionColumnChunk::TPtr> chunksInProgress;
    std::sort(Columns.begin(), Columns.end());
    for (auto&& i : Columns) {
        for (auto&& p : i.GetChunks()) {
            chunksInProgress.emplace_back(p);
        }
    }
    std::vector<TSplittedBlob> result;
    Y_ABORT_UNLESS(Settings.GetMaxBlobSize() >= 2 * Settings.GetMinBlobSize());
    while (chunksInProgress.size()) {
        i64 fullSize = 0;
        for (auto&& i : chunksInProgress) {
            fullSize += i->GetPackedSize();
        }
        if (fullSize < Settings.GetMaxBlobSize()) {
            result.emplace_back(TSplittedBlob());
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
                        result.emplace_back(TSplittedBlob());
                        for (ui32 chunk = 0; chunk < i; ++chunk) {
                            result.back().Take(chunksInProgress[chunk]);
                        }
                        Counters->BySizeSplitter.OnCorrectSerialized(result.back().GetSize());
                        chunksInProgress.erase(chunksInProgress.begin(), chunksInProgress.begin() + i);
                        hasNoSplitChanges = true;
                    } else {
                        Y_ABORT_UNLESS((i64)chunksInProgress[i]->GetPackedSize() > Settings.GetMinBlobSize() - partSize);
                        Y_ABORT_UNLESS(otherSize - (Settings.GetMinBlobSize() - partSize) >= Settings.GetMinBlobSize());

                        std::vector<IPortionColumnChunk::TPtr> newChunks;
                        const bool splittable = chunksInProgress[i]->GetRecordsCount() > 1;
                        if (splittable) {
                            Counters->BySizeSplitter.OnTrashSerialized(chunksInProgress[i]->GetPackedSize());
                            const std::vector<ui64> sizes = {(ui64)(Settings.GetMinBlobSize() - partSize)};
                            newChunks = chunksInProgress[i]->InternalSplit(Schema->GetColumnSaver(chunksInProgress[i]->GetColumnId()), Counters, sizes);
                            chunksInProgress.erase(chunksInProgress.begin() + i);
                            chunksInProgress.insert(chunksInProgress.begin() + i, newChunks.begin(), newChunks.end());
                        }

                        TSplittedBlob newBlob;
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
            Y_ABORT_UNLESS(c->GetColumnId());
            if (!lastColumnId || *lastColumnId != c->GetColumnId()) {
                Y_ABORT_UNLESS(columnIds.emplace(c->GetColumnId()).second);
                lastColumnId = c->GetColumnId();
                currentChunkIdx = 0;
            }
            c->SetChunkIdx(currentChunkIdx++);
        }
    }
    std::swap(blobs, result);
    return true;
}

TGeneralSerializedSlice::TGeneralSerializedSlice(const std::map<ui32, std::vector<IPortionColumnChunk::TPtr>>& data,
    ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings)
    : Schema(schema)
    , Counters(counters)
    , Settings(settings) {

    std::optional<ui32> recordsCount;
    for (auto&& [columnId, chunks] : data) {
        auto f = schema->GetField(columnId);
        TSplittedColumn column(f, columnId);
        column.AddChunks(chunks);
        if (!recordsCount) {
            recordsCount = column.GetRecordsCount();
        } else {
            Y_ABORT_UNLESS(*recordsCount == column.GetRecordsCount());
        }
        Size += column.GetSize();
        Columns.emplace_back(std::move(column));
    }
    Y_ABORT_UNLESS(recordsCount);
    RecordsCount = *recordsCount;
}

TGeneralSerializedSlice::TGeneralSerializedSlice(ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings)
    : Schema(schema)
    , Counters(counters)
    , Settings(settings)
{
}

TBatchSerializedSlice::TBatchSerializedSlice(std::shared_ptr<arrow::RecordBatch> batch, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings)
    : TBase(schema, counters, settings)
    , Batch(batch)
{
    Y_ABORT_UNLESS(batch);
    RecordsCount = batch->num_rows();
    Columns.reserve(batch->num_columns());
    for (auto&& i : batch->schema()->fields()) {
        TSplittedColumn c(i, schema->GetColumnId(i->name()));
        Columns.emplace_back(std::move(c));
    }

    ui32 idx = 0;
    for (auto&& i : batch->columns()) {
        auto& c = Columns[idx];
        auto columnSaver = schema->GetColumnSaver(c.GetColumnId());
        auto stats = schema->GetColumnSerializationStats(c.GetColumnId());
        TSimpleSplitter splitter(columnSaver, Counters);
        splitter.SetStats(stats);
        std::vector<IPortionColumnChunk::TPtr> chunks;
        for (auto&& i : splitter.Split(i, c.GetField(), Settings.GetMaxBlobSize())) {
            chunks.emplace_back(std::make_shared<TSplittedColumnChunk>(c.GetColumnId(), i, Schema));
        }
        c.SetChunks(chunks);
        Size += c.GetSize();
        ++idx;
    }
}

void TGeneralSerializedSlice::MergeSlice(TGeneralSerializedSlice&& slice) {
    Y_ABORT_UNLESS(Columns.size() == slice.Columns.size());
    RecordsCount += slice.GetRecordsCount();
    for (ui32 i = 0; i < Columns.size(); ++i) {
        Size += slice.Columns[i].GetSize();
        Columns[i].Merge(std::move(slice.Columns[i]));
    }
}

}
