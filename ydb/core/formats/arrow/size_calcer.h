#pragma once
#include "special_keys.h"
#include <ydb/library/accessor/accessor.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TRowSizeCalculator {
private:
    std::shared_ptr<arrow::RecordBatch> Batch;
    ui32 CommonSize = 0;
    std::vector<const arrow::BinaryArray*> BinaryColumns;
    std::vector<const arrow::StringArray*> StringColumns;
    bool Prepared = false;
    const ui32 AlignBitsCount = 1;

    ui32 GetBitWidthAligned(const ui32 bitWidth) const {
        if (AlignBitsCount == 1) {
            return bitWidth;
        }
        ui32 result = bitWidth / AlignBitsCount;
        if (bitWidth % AlignBitsCount) {
            result += 1;
        }
        result *= AlignBitsCount;
        return result;
    }

public:

    ui64 GetApproxSerializeSize(const ui64 dataSize) const {
        return Max<ui64>(dataSize * 1.05, dataSize + Batch->num_columns() * 8);
    }

    TRowSizeCalculator(const ui32 alignBitsCount)
        : AlignBitsCount(alignBitsCount)
    {

    }
    bool InitBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    ui32 GetRowBitWidth(const ui32 row) const;
    ui32 GetRowBytesSize(const ui32 row) const;
};

class TBatchSplitttingContext {
private:
    YDB_ACCESSOR(ui64, SizeLimit, 6 * 1024 * 1024);
    YDB_ACCESSOR_DEF(std::vector<TString>, FieldsForSpecialKeys);
public:
    explicit TBatchSplitttingContext(const ui64 size)
        : SizeLimit(size)
    {

    }

    void SetFieldsForSpecialKeys(const std::shared_ptr<arrow::Schema>& schema) {
        std::vector<TString> local;
        for (auto&& i : schema->fields()) {
            local.emplace_back(i->name());
        }
        std::swap(local, FieldsForSpecialKeys);
    }
};

class TSerializedBatch {
private:
    YDB_READONLY_DEF(TString, SchemaData);
    YDB_READONLY_DEF(TString, Data);
    YDB_READONLY(ui32, RowsCount, 0);
    YDB_READONLY(ui32, RawBytes, 0);
    std::optional<TFirstLastSpecialKeys> SpecialKeys;
public:
    size_t GetSize() const {
        return Data.size();
    }

    const TFirstLastSpecialKeys& GetSpecialKeysSafe() const {
        AFL_VERIFY(SpecialKeys);
        return *SpecialKeys;
    }

    bool HasSpecialKeys() const {
        return !!SpecialKeys;
    }

    TString DebugString() const;

    static bool BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context, std::vector<TSerializedBatch>& result, TString* errorMessage);
    static bool BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context, std::optional<TSerializedBatch>& sbL, std::optional<TSerializedBatch>& sbR, TString* errorMessage);
    static TSerializedBatch Build(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context);

    TSerializedBatch(TString&& schemaData, TString&& data, const ui32 rowsCount, const ui32 rawBytes, const std::optional<TFirstLastSpecialKeys>& specialKeys)
        : SchemaData(schemaData)
        , Data(data)
        , RowsCount(rowsCount)
        , RawBytes(rawBytes)
        , SpecialKeys(specialKeys)
    {

    }
};

class TSplitBlobResult {
private:
    YDB_READONLY_DEF(std::vector<TSerializedBatch>, Result);
    YDB_ACCESSOR_DEF(TString, ErrorMessage);
public:
    TSplitBlobResult(std::vector<TSerializedBatch>&& result)
        : Result(std::move(result)) {

    }
    TSplitBlobResult(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
    bool operator!() const {
        return !!ErrorMessage;
    }

    std::vector<TSerializedBatch>&& ReleaseResult() {
        return std::move(Result);
    }
};

TSplitBlobResult SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const TBatchSplitttingContext& context);

// Return size in bytes including size of bitmap mask
ui64 GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch>& batch);
// Return size in bytes including size of bitmap mask
ui64 GetArrayMemorySize(const std::shared_ptr<arrow::ArrayData>& data);
ui64 GetBatchMemorySize(const std::shared_ptr<arrow::RecordBatch>&batch);
// Return size in bytes *not* including size of bitmap mask
ui64 GetArrayDataSize(const std::shared_ptr<arrow::Array>& column);

}
