#pragma once
#include "special_keys.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/result.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_binary.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow {

class TRowSizeCalculator {
private:
    std::shared_ptr<arrow::RecordBatch> Batch;
    ui32 CommonSize = 0;
    ui32 LegacyIpcFormat = 0;
    ui32 MetadataSize = 0;
    ui32 CountColomnsWithNull = 0;
    std::vector<const arrow::BinaryArray*> BinaryColumns;
    std::vector<ui16> FixedBitWidthColumns;
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

    ui32 GetNullBitmapBytesSize(ui32 countRows) const;

    public:
        TRowSizeCalculator(const ui32 alignBitsCount)
            : AlignBitsCount(alignBitsCount) {
            FixedBitWidthColumns.resize(9, 0);
    }

    bool InitBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
    ui32 GetRowBitWidth(const ui32 row) const;
    ui32 GetRowBytesSize(const ui32 row) const;

    ui32 GetSliceSize(ui32 startIndex, ui32 length) const;
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
    std::optional<TString> SpecialKeys;
public:
    size_t GetSize() const {
        return Data.size();
    }

    const TString& GetSpecialKeysSafe() const {
        AFL_VERIFY(SpecialKeys);
        return *SpecialKeys;
    }

    bool HasSpecialKeys() const {
        return !!SpecialKeys;
    }

    TString DebugString() const;

    static TConclusion<std::vector<TSerializedBatch>> BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context);
    static TConclusionStatus BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context, std::optional<TSerializedBatch>& sbL, std::optional<TSerializedBatch>& sbR);
    static TSerializedBatch Build(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context);

    TSerializedBatch(TString&& schemaData, TString&& data, const ui32 rowsCount, const ui32 rawBytes, const std::optional<TString>& specialKeys)
        : SchemaData(schemaData)
        , Data(data)
        , RowsCount(rowsCount)
        , RawBytes(rawBytes)
        , SpecialKeys(specialKeys)
    {

    }
};

TConclusion<std::vector<TSerializedBatch>> SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const TBatchSplitttingContext& context);

// Return size in bytes including size of bitmap mask
ui64 GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch>& batch);
ui64 GetTableDataSize(const std::shared_ptr<arrow::Table>& batch);
// Return size in bytes including size of bitmap mask
ui64 GetArrayMemorySize(const std::shared_ptr<arrow::ArrayData>& data);
ui64 GetBatchMemorySize(const std::shared_ptr<arrow::RecordBatch>&batch);
ui64 GetTableMemorySize(const std::shared_ptr<arrow::Table>& batch);
// Return size in bytes *not* including size of bitmap mask
ui64 GetArrayDataSize(const std::shared_ptr<arrow::Array>& column);

}
