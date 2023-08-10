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
    std::vector<ui32> Split(const std::vector<TObject>& objects) {
        ui64 fullSize = 0;
        for (auto&& i : objects) {
            fullSize += i.GetSize();
        }
        if (fullSize <= BottomLimit) {
            return {(ui32)objects.size()};
        }
        ui64 currentSize = 0;
        ui64 currentStart = 0;
        std::vector<ui32> result;
        for (ui32 i = 0; i < objects.size(); ++i) {
            const ui64 nextSize = currentSize + objects[i].GetSize();
            const ui64 nextOtherSize = fullSize - nextSize;
            if ((nextSize >= BottomLimit && nextOtherSize >= BottomLimit) || (i + 1 == objects.size())) {
                result.emplace_back(i - currentStart + 1);
                currentSize = 0;
                currentStart = i + 1;
            } else {
                currentSize = nextSize;
            }
        }
        return result;
    }
};

TRBSplitLimiter::TRBSplitLimiter(const TGranuleMeta* /*granuleMeta*/, const NColumnShard::TIndexationCounters& counters, ISchemaDetailInfo::TPtr schemaInfo,
    const std::shared_ptr<arrow::RecordBatch> batch)
    : Counters(counters)
    , Batch(batch) {
    Y_VERIFY(Batch->num_rows());
    std::vector<TBatchSerializedSlice> slices;
    const ui32 countPacks = std::max<ui32>(1, (ui32)round(1.0 * Batch->num_rows() / TSplitSettings::MinRecordsCount));
    const ui32 stepPack = Batch->num_rows() / countPacks;
    ui32 position = 0;
    for (ui32 i = 0; i < countPacks; ++i) {
        const ui32 packSize = (i + 1 == countPacks) ? Batch->num_rows() - position : stepPack;
        std::shared_ptr<arrow::RecordBatch> current;
        current = batch->Slice(position, packSize);
        position += current->num_rows();
        TBatchSerializedSlice slice(current, schemaInfo);
        slices.emplace_back(std::move(slice));
    }
    Y_VERIFY(position == batch->num_rows());
    Y_VERIFY(slices.size() == countPacks);

    const std::vector<ui32> chunks = TSimilarSlicer(TSplitSettings::MinBlobSize).Split(slices);
    ui32 chunkStartPosition = 0;
    for (auto&& i : chunks) {
        Slices.emplace_back(std::move(slices[chunkStartPosition]));
        for (ui32 pos = chunkStartPosition + 1; pos < chunkStartPosition + i; ++pos) {
            Slices.back().MergeSlice(std::move(slices[pos]));
        }
        chunkStartPosition += i;
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
    for (auto&& i : blobs) {
        std::vector<TOrderedColumnChunk> chunksForBlob;
        for (auto&& c : i.GetChunks()) {
            chunksForBlob.emplace_back(c.GetColumnId(), c.GetData().GetRecordsCount(), c.GetData().GetSerializedChunk());
        }
        result.emplace_back(std::move(chunksForBlob));
    }
    std::swap(result, portionBlobs);
    batch = Slices.front().GetBatch();
    Slices.pop_front();
    return true;
}

}
