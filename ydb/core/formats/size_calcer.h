#pragma once
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <util/system/types.h>

namespace NKikimr::NArrow {

class TRowSizeCalculator {
private:
    std::shared_ptr<arrow::RecordBatch> Batch;
    ui32 CommonSize = 0;
    std::vector<const arrow::BinaryArray*> BinaryColumns;
    std::vector<const arrow::StringArray*> StringColumns;
    bool Prepared = false;
public:
    TRowSizeCalculator() = default;
    bool InitBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    ui32 GetRowBitWidth(const ui32 row) const;
    ui32 GetRowBytesSize(const ui32 row) const;
};

bool SplitBySize(const std::shared_ptr<arrow::RecordBatch>& batch, std::vector<std::shared_ptr<arrow::RecordBatch>>& result, const ui32 sizeLimit);

// Return size in bytes including size of bitmap mask
ui64 GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch>& batch);
// Return size in bytes *not* including size of bitmap mask
ui64 GetArrayDataSize(const std::shared_ptr<arrow::Array>& column);

}
