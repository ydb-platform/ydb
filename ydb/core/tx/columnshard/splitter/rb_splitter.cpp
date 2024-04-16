#include "rb_splitter.h"
#include "simple.h"

namespace NKikimr::NOlap {

TRBSplitLimiter::TRBSplitLimiter(std::shared_ptr<NColumnShard::TSplitterCounters> counters, ISchemaDetailInfo::TPtr schemaInfo,
    const std::shared_ptr<arrow::RecordBatch> batch, const TSplitSettings& settings)
    : Counters(counters)
    , Batch(batch)
    , Settings(settings)
{
    Y_ABORT_UNLESS(Batch->num_rows());
    std::vector<TBatchSerializedSlice> slices = BuildSimpleSlices(Batch, Settings, Counters, schemaInfo);

    auto chunks = TSimilarSlicer(Settings.GetMinBlobSize()).Split(slices);
    ui32 chunkStartPosition = 0;
    for (auto&& spanObjects : chunks) {
        Slices.emplace_back(TBatchSerializedSlice(std::move(spanObjects)));
        chunkStartPosition += spanObjects.size();
    }
    Y_ABORT_UNLESS(chunkStartPosition == slices.size());
    ui32 recordsCountCheck = 0;
    for (auto&& i : Slices) {
        recordsCountCheck += i.GetRecordsCount();
    }
    Y_ABORT_UNLESS(recordsCountCheck == batch->num_rows());
}

bool TRBSplitLimiter::Next(std::vector<std::vector<std::shared_ptr<IPortionDataChunk>>>& portionBlobs, std::shared_ptr<arrow::RecordBatch>& batch, const TEntityGroups& groups) {
    if (!Slices.size()) {
        return false;
    }
    std::vector<TSplittedBlob> blobs;
    Slices.front().GroupBlobs(blobs, groups);
    std::vector<std::vector<std::shared_ptr<IPortionDataChunk>>> result;
    std::map<ui32, ui32> columnChunks;
    for (auto&& i : blobs) {
        if (blobs.size() == 1) {
            Counters->MonoBlobs.OnBlobData(i.GetSize());
        } else {
            Counters->SplittedBlobs.OnBlobData(i.GetSize());
        }
        result.emplace_back(i.GetChunks());
    }
    std::swap(result, portionBlobs);
    batch = Slices.front().GetBatch();
    Slices.pop_front();
    return true;
}

std::vector<NKikimr::NOlap::TBatchSerializedSlice> TRBSplitLimiter::BuildSimpleSlices(const std::shared_ptr<arrow::RecordBatch>& batch, const TSplitSettings& settings,
    const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const ISchemaDetailInfo::TPtr& schemaInfo)
{
    std::vector<TBatchSerializedSlice> slices;
    auto stats = schemaInfo->GetBatchSerializationStats(batch);
    ui32 recordsCount = settings.GetMinRecordsCount();
    if (stats) {
        const ui32 recordsCountForMinSize = stats->PredictOptimalPackRecordsCount(batch->num_rows(), settings.GetMinBlobSize()).value_or(recordsCount);
        const ui32 recordsCountForMaxPortionSize = stats->PredictOptimalPackRecordsCount(batch->num_rows(), settings.GetMaxPortionSize()).value_or(recordsCount);
        recordsCount = std::min(recordsCountForMaxPortionSize, std::max(recordsCount, recordsCountForMinSize));
    }
    auto linearSplitInfo = TSimpleSplitter::GetOptimalLinearSplitting(batch->num_rows(), recordsCount);
    for (auto it = linearSplitInfo.StartIterator(); it.IsValid(); it.Next()) {
        std::shared_ptr<arrow::RecordBatch> current = batch->Slice(it.GetPosition(), it.GetCurrentPackSize());
        TBatchSerializedSlice slice(current, schemaInfo, counters, settings);
        slices.emplace_back(std::move(slice));
    }
    return slices;
}

}
