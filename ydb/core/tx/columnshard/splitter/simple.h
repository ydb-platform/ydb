#pragma once
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>

namespace NKikimr::NOlap {

class TSaverSplittedChunk {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, SlicedBatch);
    YDB_READONLY_DEF(TString, SerializedChunk);
public:
    ui32 GetRecordsCount() const {
        return SlicedBatch->num_rows();
    }

    TSaverSplittedChunk() = default;

    TSaverSplittedChunk(std::shared_ptr<arrow::RecordBatch> batch, TString&& serializedChunk)
        : SlicedBatch(batch)
        , SerializedChunk(std::move(serializedChunk)) {

    }
};

class TSimpleSplitter {
private:
    TColumnSaver ColumnSaver;
public:
    TSimpleSplitter(const TColumnSaver& columnSaver)
        : ColumnSaver(columnSaver)
    {

    }

    std::vector<TSaverSplittedChunk> Split(std::shared_ptr<arrow::Array> data, std::shared_ptr<arrow::Field> field, const ui32 maxBlobSize) const;
    std::vector<TSaverSplittedChunk> Split(std::shared_ptr<arrow::RecordBatch> data, const ui32 maxBlobSize) const;
    std::vector<TSaverSplittedChunk> SplitByRecordsCount(std::shared_ptr<arrow::RecordBatch> data, const std::vector<ui64>& recordsCount) const;
    std::vector<TSaverSplittedChunk> SplitBySizes(std::shared_ptr<arrow::RecordBatch> data, const TString& dataSerialization, const std::vector<ui64>& splitPartSizesExt) const;
};

}
