#include "batch_slice.h"

namespace NKikimr::NOlap {


bool TBatchSerializedSlice::GroupBlobs(std::vector<TSplittedBlob>& blobs) {
    std::vector<TSplittedColumnChunk> chunksInProgress;
    std::vector<TSplittedColumnChunk> chunksReady;
    std::sort(Columns.begin(), Columns.end());
    for (auto&& i : Columns) {
        for (auto&& p : i.GetChunks()) {
            chunksInProgress.emplace_back(p);
        }
    }
    std::vector<TSplittedBlob> result;
    Y_VERIFY(TSplitSettings::MaxBlobSize >= 2 * TSplitSettings::MinBlobSize);
    while (chunksInProgress.size()) {
        ui64 fullSize = 0;
        for (auto&& i : chunksInProgress) {
            fullSize += i.GetSize();
        }
        if (fullSize < TSplitSettings::MaxBlobSize) {
            result.emplace_back(TSplittedBlob());
            for (auto&& i : chunksInProgress) {
                Y_VERIFY(result.back().Take(i));
            }
            chunksInProgress.clear();
            break;
        }
        bool hasNoSplitChanges = true;
        while (hasNoSplitChanges) {
            hasNoSplitChanges = false;
            ui64 partSize = 0;
            for (ui32 i = 0; i < chunksInProgress.size(); ++i) {
                const ui64 nextPartSize = partSize + chunksInProgress[i].GetSize();
                const ui64 nextOtherSize = fullSize - nextPartSize;
                const ui64 otherSize = fullSize - partSize;
                if (nextPartSize >= TSplitSettings::MaxBlobSize || nextOtherSize < TSplitSettings::MinBlobSize) {
                    Y_VERIFY(otherSize >= TSplitSettings::MinBlobSize);
                    Y_VERIFY(partSize < TSplitSettings::MaxBlobSize);
                    if (partSize >= TSplitSettings::MinBlobSize) {
                        result.emplace_back(TSplittedBlob());
                        for (ui32 chunk = 0; chunk < i; ++chunk) {
                            Y_VERIFY(result.back().Take(chunksInProgress[chunk]));
                        }
                        chunksInProgress.erase(chunksInProgress.begin(), chunksInProgress.begin() + i);
                        hasNoSplitChanges = true;
                    } else {
                        Y_VERIFY(chunksInProgress[i].GetSize() > TSplitSettings::MinBlobSize - partSize);
                        Y_VERIFY(otherSize - (TSplitSettings::MinBlobSize - partSize) >= TSplitSettings::MinBlobSize);
                        chunksInProgress[i].AddSplit(TSplitSettings::MinBlobSize - partSize);
                    }
                    if (!hasNoSplitChanges) {
                        std::vector<TSplittedColumnChunk> newChunks = chunksInProgress[i].InternalSplit(Schema->GetColumnSaver(chunksInProgress[i].GetColumnId()));
                        chunksInProgress.erase(chunksInProgress.begin() + i);
                        chunksInProgress.insert(chunksInProgress.begin() + i, newChunks.begin(), newChunks.end());

                        result.emplace_back(TSplittedBlob());
                        for (ui32 chunk = 0; chunk <= i; ++chunk) {
                            Y_VERIFY(result.back().Take(chunksInProgress[chunk]));
                        }
                        if (result.back().GetSize() < TSplitSettings::MaxBlobSize) {
                            chunksInProgress.erase(chunksInProgress.begin(), chunksInProgress.begin() + i + 1);
                        } else {
                            result.pop_back();
                        }
                    }
                    break;
                }
                partSize = nextPartSize;
            }
        }
    }
    std::swap(blobs, result);
    return true;
}

TBatchSerializedSlice::TBatchSerializedSlice(std::shared_ptr<arrow::RecordBatch> batch, ISchemaDetailInfo::TPtr schema)
    : Schema(schema)
    , Batch(batch)
{
    Y_VERIFY(batch);
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
        c.SetBlobs(TSimpleSplitter(columnSaver).Split(i, c.GetField(), TSplitSettings::MaxBlobSize));
        Size += c.GetSize();
        ++idx;
    }
}

void TBatchSerializedSlice::MergeSlice(TBatchSerializedSlice&& slice) {
    Batch = NArrow::CombineBatches({Batch, slice.Batch});
    Y_VERIFY(Columns.size() == slice.Columns.size());
    RecordsCount += slice.GetRecordsCount();
    for (ui32 i = 0; i < Columns.size(); ++i) {
        Size += slice.Columns[i].GetSize();
        Columns[i].Merge(std::move(slice.Columns[i]));
    }
}

}
