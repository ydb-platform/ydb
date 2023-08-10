#include "simple.h"

namespace NKikimr::NOlap {

std::vector<NKikimr::NOlap::TSaverSplittedChunk> TSimpleSplitter::Split(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field, const ui32 maxBlobSize) const {
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
    auto batch = arrow::RecordBatch::Make(schema, data->length(), {data});
    return Split(batch, maxBlobSize);
}

std::vector<NKikimr::NOlap::TSaverSplittedChunk> TSimpleSplitter::Split(std::shared_ptr<arrow::RecordBatch> data, const ui32 maxBlobSize) const {
    Y_VERIFY(data->num_columns() == 1);
    ui32 splitFactor = 1;
    while (true) {
        Y_VERIFY(splitFactor < 100);
        std::vector<TSaverSplittedChunk> result;
        result.reserve(splitFactor);
        bool isCorrect = true;
        if (splitFactor == 1) {
            TString blob = ColumnSaver.Apply(data);
            isCorrect = blob.size() < maxBlobSize;
            result.emplace_back(TSaverSplittedChunk(data, std::move(blob)));
        } else {
            const ui32 sliceSize = data->num_rows() / splitFactor;
            Y_VERIFY(sliceSize);
            ui32 idx = 0;
            for (ui32 i = 0; i < data->num_rows();) {
                const ui32 sliceCurrentSize = (++idx == splitFactor) ? data->num_rows() - i : sliceSize;
                Y_VERIFY(sliceCurrentSize);
                auto slice = data->Slice(i, sliceCurrentSize);
                result.emplace_back(slice, ColumnSaver.Apply(slice));
                if (result.back().GetSerializedChunk().size() >= maxBlobSize) {
                    isCorrect = false;
                    break;
                }
                i += slice->num_rows();
            }
        }
        if (isCorrect) {
            return result;
        }
        ++splitFactor;
    }
}

std::vector<NKikimr::NOlap::TSaverSplittedChunk> TSimpleSplitter::SplitByRecordsCount(std::shared_ptr<arrow::RecordBatch> data, const std::vector<ui64>& recordsCount) const {
    std::vector<TSaverSplittedChunk> result;
    ui64 position = 0;
    for (auto&& i : recordsCount) {
        auto subData = data->Slice(position, i);
        result.emplace_back(subData, ColumnSaver.Apply(subData));
        position += i;
    }
    Y_VERIFY(position == (ui64)data->num_rows());
    return result;
}

std::vector<NKikimr::NOlap::TSaverSplittedChunk> TSimpleSplitter::SplitBySizes(std::shared_ptr<arrow::RecordBatch> data, const TString& dataSerialization, const std::vector<ui64>& splitPartSizesExt) const {
    auto splitPartSizesLocal = splitPartSizesExt;
    Y_VERIFY(data);
    {
        ui32 sumSizes = 0;
        for (auto&& i : splitPartSizesExt) {
            sumSizes += i;
        }
        Y_VERIFY(sumSizes <= dataSerialization.size());

        if (sumSizes < dataSerialization.size()) {
            splitPartSizesLocal.emplace_back(dataSerialization.size() - sumSizes);
        }
    }
    Y_VERIFY(splitPartSizesLocal.size() <= (ui64)data->num_rows());
    std::vector<ui64> recordsCount;
    i64 remainedRecordsCount = data->num_rows();
    const double rowsPerByte = 1.0 * data->num_rows() / dataSerialization.size();
    for (ui32 idx = 0; idx < splitPartSizesLocal.size(); ++idx) {
        i64 expectedRecordsCount = rowsPerByte * splitPartSizesLocal[idx];
        if (expectedRecordsCount < 1) {
            expectedRecordsCount = 1;
        } else if (remainedRecordsCount < expectedRecordsCount + (i64)splitPartSizesLocal.size()) {
            expectedRecordsCount = remainedRecordsCount - splitPartSizesLocal.size();
            Y_VERIFY(expectedRecordsCount >= 0);
        }
        if (idx + 1 == splitPartSizesLocal.size()) {
            expectedRecordsCount = remainedRecordsCount;
        }
        Y_VERIFY(expectedRecordsCount);
        recordsCount.emplace_back(expectedRecordsCount);
        remainedRecordsCount -= expectedRecordsCount;
        Y_VERIFY(remainedRecordsCount >= 0);
    }
    Y_VERIFY(remainedRecordsCount == 0);
    return SplitByRecordsCount(data, recordsCount);
}

}
