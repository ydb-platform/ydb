#include "simple.h"
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/common/validation.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

std::vector<std::shared_ptr<IPortionDataChunk>> TSplittedColumnChunk::DoInternalSplitImpl(const TColumnSaver& saver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters, const std::vector<ui64>& splitSizes) const {
    auto chunks = TSimpleSplitter(saver, counters).SplitBySizes(Data.GetSlicedBatch(), Data.GetSerializedChunk(), splitSizes);
    std::vector<std::shared_ptr<IPortionDataChunk>> newChunks;
    for (auto&& i : chunks) {
        newChunks.emplace_back(std::make_shared<TSplittedColumnChunk>(GetColumnId(), i, SchemaInfo));
    }
    return newChunks;
}

TString TSplittedColumnChunk::DoDebugString() const {
    return TStringBuilder() << "records_count=" << GetRecordsCount() << ";data=" << NArrow::DebugJson(Data.GetSlicedBatch(), 3, 3) << ";";
}

ui64 TSplittedColumnChunk::DoGetRawBytesImpl() const {
    return NArrow::GetBatchDataSize(Data.GetSlicedBatch());
}

std::vector<TSaverSplittedChunk> TSimpleSplitter::Split(const std::shared_ptr<arrow::Array>& data, const std::shared_ptr<arrow::Field>& field, const ui32 maxBlobSize) const {
    AFL_VERIFY(data);
    AFL_VERIFY(field);
    auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
    auto batch = arrow::RecordBatch::Make(schema, data->length(), {data});
    return Split(batch, maxBlobSize);
}

class TSplitChunk {
private:
    std::shared_ptr<arrow::RecordBatch> Data;
    YDB_READONLY_DEF(std::optional<TSaverSplittedChunk>, Result);
    ui32 SplitFactor = 0;
    ui32 Iterations = 0;
    ui32 MaxBlobSize = 8 * 1024 * 1024;
    TColumnSaver ColumnSaver;
    std::shared_ptr<NColumnShard::TSplitterCounters> Counters;
public:
    TSplitChunk(const ui32 baseSplitFactor, const ui32 maxBlobSize, const std::shared_ptr<arrow::RecordBatch>& data, const TColumnSaver& columnSaver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters)
        : Data(data)
        , SplitFactor(baseSplitFactor)
        , MaxBlobSize(maxBlobSize)
        , ColumnSaver(columnSaver)
        , Counters(counters)
    {
        AFL_VERIFY(Data && Data->num_rows());
        AFL_VERIFY(SplitFactor);
    }

    TSplitChunk(const ui32 baseSplitFactor, const ui32 maxBlobSize, const std::shared_ptr<arrow::RecordBatch>& data, TString&& serializedData, const TColumnSaver& columnSaver, const std::shared_ptr<NColumnShard::TSplitterCounters>& counters)
        : Data(data)
        , Result(TSaverSplittedChunk(data, std::move(serializedData)))
        , SplitFactor(baseSplitFactor)
        , MaxBlobSize(maxBlobSize)
        , ColumnSaver(columnSaver)
        , Counters(counters)
    {
        AFL_VERIFY(Data && Data->num_rows());
        AFL_VERIFY(SplitFactor);
    }

    std::vector<TSplitChunk> Split() {
        while (true) {
            AFL_VERIFY(!Result);
            AFL_VERIFY(++Iterations < 100);
            AFL_VERIFY(SplitFactor <= Data->num_rows())("factor", SplitFactor)("records", Data->num_rows())("iteration", Iterations)("size", NArrow::GetBatchDataSize(Data));
            bool found = false;
            std::vector<TSplitChunk> result;
            if (SplitFactor == 1) {
                TString blob = ColumnSaver.Apply(Data);
                if (blob.size() < MaxBlobSize) {
                    Counters->SimpleSplitter.OnCorrectSerialized(blob.size());
                    Result = TSaverSplittedChunk(Data, std::move(blob));
                    found = true;
                    result.emplace_back(*this);
                } else {
                    Counters->SimpleSplitter.OnTrashSerialized(blob.size());
                    TBatchSerializationStat stats(blob.size(), Data->num_rows(), NArrow::GetBatchDataSize(Data));
                    SplitFactor = stats.PredictOptimalSplitFactor(Data->num_rows(), MaxBlobSize).value_or(1);
                    if (SplitFactor == 1) {
                        SplitFactor = 2;
                    }
                    AFL_VERIFY(Data->num_rows() > 1);
                }
            } else {
                TLinearSplitInfo linearSplitting = TSimpleSplitter::GetLinearSplittingByMax(Data->num_rows(), Data->num_rows() / SplitFactor);
                TStringBuilder sb;
                std::optional<ui32> badStartPosition;
                ui32 badBatchRecordsCount = 0;
                ui64 badBatchSerializedSize = 0;
                ui32 badBatchCount = 0;
                for (auto it = linearSplitting.StartIterator(); it.IsValid(); it.Next()) {
                    auto slice = Data->Slice(it.GetPosition(), it.GetCurrentPackSize());
                    TString blob = ColumnSaver.Apply(slice);
                    if (blob.size() >= MaxBlobSize) {
                        Counters->SimpleSplitter.OnTrashSerialized(blob.size());
                        if (!badStartPosition) {
                            badStartPosition = it.GetPosition();
                        }
                        badBatchSerializedSize += blob.size();
                        badBatchRecordsCount += it.GetCurrentPackSize();
                        ++badBatchCount;
                        Y_ABORT_UNLESS(!linearSplitting.IsMinimalGranularity());
                    } else {
                        Counters->SimpleSplitter.OnCorrectSerialized(blob.size());
                        if (badStartPosition) {
                            AFL_VERIFY(badBatchRecordsCount && badBatchCount)("count", badBatchCount)("records", badBatchRecordsCount);
                            auto badSlice = Data->Slice(*badStartPosition, badBatchRecordsCount);
                            TBatchSerializationStat stats(badBatchSerializedSize, badBatchRecordsCount, Max<ui32>());
                            result.emplace_back(std::max<ui32>(stats.PredictOptimalSplitFactor(badBatchRecordsCount, MaxBlobSize).value_or(1), badBatchCount) + 1, MaxBlobSize, badSlice, ColumnSaver, Counters);
                            badStartPosition = {};
                            badBatchRecordsCount = 0;
                            badBatchCount = 0;
                            badBatchSerializedSize = 0;
                        }
                        found = true;
                        result.emplace_back(1, MaxBlobSize, slice, std::move(blob), ColumnSaver, Counters);
                    }
                }
                if (badStartPosition) {
                    auto badSlice = Data->Slice(*badStartPosition, badBatchRecordsCount);
                    TBatchSerializationStat stats(badBatchSerializedSize, badBatchRecordsCount, Max<ui32>());
                    result.emplace_back(std::max<ui32>(stats.PredictOptimalSplitFactor(badBatchRecordsCount, MaxBlobSize).value_or(1), badBatchCount) + 1, MaxBlobSize, badSlice, ColumnSaver, Counters);
                }
                ++SplitFactor;
            }
            if (found) {
                return result;
            }
        }
        AFL_VERIFY(false);
        return {};
    }
};

std::vector<TSaverSplittedChunk> TSimpleSplitter::Split(const std::shared_ptr<arrow::RecordBatch>& data, const ui32 maxBlobSize) const {
    AFL_VERIFY(data->num_columns() == 1);
    AFL_VERIFY(data->num_rows());
    TSplitChunk baseChunk(Stats ? Stats->PredictOptimalSplitFactor(data->num_rows(), maxBlobSize).value_or(1) : 1, maxBlobSize, data, ColumnSaver, Counters);
    std::vector<TSplitChunk> chunks = {baseChunk};
    for (auto it = chunks.begin(); it != chunks.end(); ) {
        AFL_VERIFY(chunks.size() < 100);
        if (!!it->GetResult()) {
            ++it;
            continue;
        }
        std::vector<TSplitChunk> splitted = it->Split();
        if (splitted.size() == 1) {
            *it = splitted.front();
        } else {
            it = chunks.insert(it, splitted.begin(), splitted.end());
            chunks.erase(it + splitted.size());
        }
    }
    std::vector<TSaverSplittedChunk> result;
    for (auto&& i : chunks) {
        AFL_VERIFY(i.GetResult());
        result.emplace_back(*i.GetResult());
    }
    return result;
}

std::vector<TSaverSplittedChunk> TSimpleSplitter::SplitByRecordsCount(std::shared_ptr<arrow::RecordBatch> data, const std::vector<ui64>& recordsCount) const {
    std::vector<TSaverSplittedChunk> result;
    ui64 position = 0;
    for (auto&& i : recordsCount) {
        auto subData = data->Slice(position, i);
        result.emplace_back(subData, ColumnSaver.Apply(subData));
        position += i;
    }
    Y_ABORT_UNLESS(position == (ui64)data->num_rows());
    return result;
}

std::vector<TSaverSplittedChunk> TSimpleSplitter::SplitBySizes(std::shared_ptr<arrow::RecordBatch> data, const TString& dataSerialization, const std::vector<ui64>& splitPartSizesExt) const {
    auto splitPartSizesLocal = splitPartSizesExt;
    Y_ABORT_UNLESS(data);
    {
        ui32 sumSizes = 0;
        for (auto&& i : splitPartSizesExt) {
            sumSizes += i;
        }
        Y_ABORT_UNLESS(sumSizes <= dataSerialization.size());

        if (sumSizes < dataSerialization.size()) {
            splitPartSizesLocal.emplace_back(dataSerialization.size() - sumSizes);
        }
    }
    std::vector<ui64> recordsCount;
    i64 remainedRecordsCount = data->num_rows();
    const double rowsPerByte = 1.0 * data->num_rows() / dataSerialization.size();
    i32 remainedParts = splitPartSizesLocal.size();
    for (ui32 idx = 0; idx < splitPartSizesLocal.size(); ++idx) {
        AFL_VERIFY(remainedRecordsCount >= remainedParts)("remained_records_count", remainedRecordsCount)
            ("remained_parts", remainedParts)("idx", idx)("size", splitPartSizesLocal.size())("sizes", JoinSeq(",", splitPartSizesLocal))("data_size", dataSerialization.size());
        --remainedParts;
        i64 expectedRecordsCount = rowsPerByte * splitPartSizesLocal[idx];
        if (expectedRecordsCount < 1) {
            expectedRecordsCount = 1;
        } else if (remainedRecordsCount < expectedRecordsCount + remainedParts) {
            expectedRecordsCount = remainedRecordsCount - remainedParts;
        }
        if (idx + 1 == splitPartSizesLocal.size()) {
            expectedRecordsCount = remainedRecordsCount;
        }
        Y_ABORT_UNLESS(expectedRecordsCount);
        recordsCount.emplace_back(expectedRecordsCount);
        remainedRecordsCount -= expectedRecordsCount;
        Y_ABORT_UNLESS(remainedRecordsCount >= 0);
    }
    Y_ABORT_UNLESS(remainedRecordsCount == 0);
    return SplitByRecordsCount(data, recordsCount);
}

std::shared_ptr<arrow::Scalar> TSaverSplittedChunk::GetFirstScalar() const {
    return NArrow::TStatusValidator::GetValid(SlicedBatch->column(0)->GetScalar(0));
}

std::shared_ptr<arrow::Scalar> TSaverSplittedChunk::GetLastScalar() const {
    return NArrow::TStatusValidator::GetValid(SlicedBatch->column(0)->GetScalar(GetRecordsCount() - 1));
}

}
