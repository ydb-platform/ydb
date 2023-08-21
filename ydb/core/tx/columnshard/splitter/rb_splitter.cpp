#include "rb_splitter.h"

namespace NKikimr::NOlap {

class TSimilarSlicer {
private:
    const ui64 BottomLimit = 0;
public:
    TSimilarSlicer(const ui64 bottomLimit)
        : BottomLimit(bottomLimit)
    {

    }

    template <class TObject>
    std::vector<TVectorView<TObject>> Split(std::vector<TObject>& objects) {
        ui64 fullSize = 0;
        for (auto&& i : objects) {
            fullSize += i.GetSize();
        }
        if (fullSize <= BottomLimit) {
            return {TVectorView<TObject>(objects.begin(), objects.end())};
        }
        ui64 currentSize = 0;
        ui64 currentStart = 0;
        std::vector<TVectorView<TObject>> result;
        for (ui32 i = 0; i < objects.size(); ++i) {
            const ui64 nextSize = currentSize + objects[i].GetSize();
            const ui64 nextOtherSize = fullSize - nextSize;
            if ((nextSize >= BottomLimit && nextOtherSize >= BottomLimit) || (i + 1 == objects.size())) {
                result.emplace_back(TVectorView<TObject>(objects.begin() + currentStart, objects.begin() + i + 1));
                currentSize = 0;
                currentStart = i + 1;
            } else {
                currentSize = nextSize;
            }
        }
        return result;
    }
};

TRBSplitLimiter::TRBSplitLimiter(std::shared_ptr<NColumnShard::TSplitterCounters> counters, ISchemaDetailInfo::TPtr schemaInfo,
    const std::shared_ptr<arrow::RecordBatch> batch, const TSplitSettings& settings)
    : Counters(counters)
    , Batch(batch)
    , Settings(settings)
{
    Y_VERIFY(Batch->num_rows());
    std::vector<TBatchSerializedSlice> slices;
    auto stats = schemaInfo->GetBatchSerializationStats(Batch);
    ui32 recordsCount = Settings.GetMinRecordsCount();
    if (stats) {
        const ui32 recordsCountForMinSize = stats->PredictOptimalPackRecordsCount(Batch->num_rows(), Settings.GetMinBlobSize()).value_or(recordsCount);
        const ui32 recordsCountForMaxPortionSize = stats->PredictOptimalPackRecordsCount(Batch->num_rows(), Settings.GetMaxPortionSize()).value_or(recordsCount);
        recordsCount = std::min(recordsCountForMaxPortionSize, std::max(recordsCount, recordsCountForMinSize));
    }
    auto linearSplitInfo = TSimpleSplitter::GetOptimalLinearSplitting(Batch->num_rows(), recordsCount);
    for (auto it = linearSplitInfo.StartIterator(); it.IsValid(); it.Next()) {
        std::shared_ptr<arrow::RecordBatch> current = batch->Slice(it.GetPosition(), it.GetCurrentPackSize());
        TBatchSerializedSlice slice(current, schemaInfo, Counters, settings);
        slices.emplace_back(std::move(slice));
    }

    auto chunks = TSimilarSlicer(Settings.GetMinBlobSize()).Split(slices);
    ui32 chunkStartPosition = 0;
    for (auto&& spanObjects : chunks) {
        Slices.emplace_back(TBatchSerializedSlice(std::move(spanObjects)));
        chunkStartPosition += spanObjects.size();
    }
    Y_VERIFY(chunkStartPosition == slices.size());
    ui32 recordsCountCheck = 0;
    for (auto&& i : Slices) {
        recordsCountCheck += i.GetRecordsCount();
    }
    Y_VERIFY(recordsCountCheck == batch->num_rows());
}

bool TRBSplitLimiter::Next(std::vector<std::vector<TOrderedColumnChunk>>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!Slices.size()) {
        return false;
    }
    std::vector<TSplittedBlob> blobs;
    Slices.front().GroupBlobs(blobs);
    std::vector<std::vector<TOrderedColumnChunk>> result;
    std::map<ui32, ui32> columnChunks;
    for (auto&& i : blobs) {
        if (blobs.size() == 1) {
            Counters->MonoBlobs.OnBlobData(i.GetSize());
        } else {
            Counters->SplittedBlobs.OnBlobData(i.GetSize());
        }
        std::vector<TOrderedColumnChunk> chunksForBlob;
        ui64 offset = 0;
        for (auto&& c : i.GetChunks()) {
            chunksForBlob.emplace_back(TChunkAddress(c.GetColumnId(), columnChunks[c.GetColumnId()]++), offset, c.GetData().GetSerializedChunk(), c.GetData().GetColumn());
            offset += c.GetSize();
        }
        result.emplace_back(std::move(chunksForBlob));
    }
    std::swap(result, portionBlobs);
    batch = Slices.front().GetBatch();
    Slices.pop_front();
    return true;
}

}
