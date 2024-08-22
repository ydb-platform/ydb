#include "arrow_helpers.h"
#include "size_calcer.h"
#include "switch_type.h"

#include "dictionary/conversion.h"

#include <arrow/ipc/util.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

ui32 GetCountRowsForSlice(const TRowSizeCalculator& rowCalculator, ui32 left, ui32 right, ui32 limit) {
    ui32 l = left;
    ui32 r = right;
    while (l <= r) {
        ui32 mid = (r + l) / 2;
        ui32 currentSize = rowCalculator.GetSliceSize(left, mid - left);
        if (currentSize == limit) {
            return mid - left;
        }
        if (currentSize < limit) {
            l = mid + 1;
        } else {
            r = mid - 1;
        }
    }
    return l - left - 1;
}

TConclusion<std::vector<TSerializedBatch>> SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const TBatchSplitttingContext& context) {
    if (GetBatchDataSize(batch) <= context.GetSizeLimit()) {
        return TSerializedBatch::BuildWithLimit(batch, context);
    }
    TRowSizeCalculator rowCalculator(8);
    if (!rowCalculator.InitBatch(batch)) {
        return TConclusionStatus::Fail("unexpected column type on batch initialization for row size calculator");
    }

    std::vector<TSerializedBatch> result;
    ui32 startIdx = 0;
    ui32 numRows = batch->num_rows();
    while (startIdx < numRows) {
        ui32 countRows = GetCountRowsForSlice(rowCalculator, startIdx, numRows, context.GetSizeLimit());
        if (countRows == 0) {
            return TConclusionStatus::Fail("there is row with size + metadata more then limit (" + ::ToString(context.GetSizeLimit()) + ")");
        }
        auto localResult = TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, countRows), context);
        if (localResult.IsFail()) {
            return TConclusionStatus::Fail(
                "cannot build blobs for batch slice (" + ::ToString(countRows) + " rows): " + localResult.GetErrorMessage());
        } else {
            result.insert(result.end(), localResult.GetResult().begin(), localResult.GetResult().end());
        }
        startIdx += countRows;
    }
    return result;
}

ui32 TRowSizeCalculator::GetRowBitWidth(const ui32 row) const {
    Y_ABORT_UNLESS(Prepared);
    ui32 result = CommonSize;
    for (auto&& c : BinaryColumns) {
        result += GetBitWidthAligned(c->GetView(row).size() * 8);
    }
    return result;
}

bool TRowSizeCalculator::InitBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    Batch = batch;
    CommonSize = 0;
    BinaryColumns.clear();

    CountColomnsWithNull = 0;
    LegacyIpcFormat = 8;  // write_legacy_ipc_format in IpcWriteOptions. If value true, then need change on 4, by default false
    MetadataSize = 72;  // initial state
    MetadataSize += 16;  // two numbers for size of metadata and number of rows in the batch
    for (ui32 i = 0; i < 9; i++) {
        FixedBitWidthColumns[i] = 0;
    }

    Prepared = false;
    for (ui32 i = 0; i < (ui32)Batch->num_columns(); ++i) {
        auto currentColumn = Batch->column(i);
        // FieldMetadata (24 bytes) + BufferMetadata (16 bytes) + NullBitmap (8 bytes)
        MetadataSize += 48;
        auto fSize = std::dynamic_pointer_cast<arrow::FixedWidthType>(currentColumn->type());
        if (fSize) {
            for (ui32 j = 0; j < 9; j++) {
                if ((1 << j) == fSize->bit_width()) {
                    FixedBitWidthColumns[j]++;
                    break;
                }
            }
            CommonSize += GetBitWidthAligned(fSize->bit_width());
        } else {
            MetadataSize += 16;
            if (currentColumn->type()->id() == arrow::Type::BINARY || currentColumn->type()->id() == arrow::Type::STRING) {
                const arrow::BinaryArray& viewArray = static_cast<const arrow::BinaryArray&>(*currentColumn);
                BinaryColumns.emplace_back(&viewArray);
            } else {
                return false;
            }
        }
        if (currentColumn->null_count() != 0) {
            CountColomnsWithNull++;
        }
    }
    CommonSize = GetBitWidthAligned(CommonSize);
    Prepared = true;
    return true;
}

ui32 TRowSizeCalculator::GetRowBytesSize(const ui32 row) const {
    const ui32 bitsWidth = GetRowBitWidth(row);
    ui32 result = bitsWidth / 8;
    if (bitsWidth % 8) {
        ++result;
    }
    return result;
}

ui32 TRowSizeCalculator::GetNullBitmapBytesSize(ui32 countRows) const {
    ui32 result = countRows / 64;
    if (countRows % 64 != 0) {
        result++;
    }
    return CountColomnsWithNull * result * 8;
}

int64_t RoundUp(int64_t value, int64_t factor) {
    if (factor == 8) {
        return arrow::BitUtil::RoundUpToMultipleOf8(value);
    } else if (factor == 64) {
        return arrow::BitUtil::RoundUpToMultipleOf64(value);
    }
    return arrow::BitUtil::RoundUp(value, factor);
}

ui32 TRowSizeCalculator::GetSliceSize(ui32 startIndex, ui32 length) const {
    Y_ABORT_UNLESS(Prepared);
    ui32 bodySize = 0;

    for (ui32 i = 0; i < 9; i++) {
        bodySize += FixedBitWidthColumns[i] * RoundUp((1 << i) * length, 64) / 8;
    }

    for (const auto& viewArray : BinaryColumns) {
        int64_t total_data_bytes = viewArray->value_offset(length + startIndex) - viewArray->value_offset(startIndex);
        const int64_t slice_length =
            std::min(arrow::ipc::PaddedLength(total_data_bytes), viewArray->value_data()->size() - viewArray->value_offset(startIndex));
        bodySize += RoundUp(slice_length, 8) + RoundUp((length + 1) * sizeof(arrow::StringArray::offset_type), 8);
    }

    bodySize += GetNullBitmapBytesSize(length);
    return bodySize + MetadataSize + LegacyIpcFormat;
}

ui64 GetArrayMemorySize(const std::shared_ptr<arrow::ArrayData>& data) {
    if (!data) {
        return 0;
    }
    ui64 result = 0;
    for (auto&& i : data->buffers) {
        if (i) {
            result += i->capacity();
        }
    }
    for (auto&& i : data->child_data) {
        for (auto&& b : i->buffers) {
            if (b) {
                result += b->capacity();
            }
        }
    }
    if (data->dictionary) {
        for (auto&& b : data->dictionary->buffers) {
            if (b) {
                result += b->capacity();
            }
        }
    }
    return result;
}


ui64 GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return 0;
    }
    ui64 bytes = 0;
    for (auto& column : batch->columns()) {
        bytes += GetArrayDataSize(column);
    }
    return bytes;
}

ui64 GetBatchMemorySize(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return 0;
    }
    ui64 bytes = 0;
    for (auto& column : batch->column_data()) {
        bytes += GetArrayMemorySize(column);
    }
    return bytes;
}

ui64 GetTableMemorySize(const std::shared_ptr<arrow::Table>& batch) {
    if (!batch) {
        return 0;
    }
    ui64 bytes = 0;
    for (auto& column : batch->columns()) {
        for (auto&& chunk : column->chunks()) {
            bytes += GetArrayMemorySize(chunk->data());
        }
    }
    return bytes;
}

ui64 GetTableDataSize(const std::shared_ptr<arrow::Table>& batch) {
    if (!batch) {
        return 0;
    }
    ui64 bytes = 0;
    for (auto& column : batch->columns()) {
        for (auto&& chunk : column->chunks()) {
            bytes += GetArrayDataSize(chunk);
        }
    }
    return bytes;
}

template <typename TType>
ui64 GetArrayDataSizeImpl(const std::shared_ptr<arrow::Array>& column) {
    return sizeof(typename TType::c_type) * column->length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::NullType>(const std::shared_ptr<arrow::Array>& column) {
    return column->length() * 8; // Special value for empty lines
}

template <>
ui64 GetArrayDataSizeImpl<arrow::StringType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::StringArray>(column);
    return typedColumn->total_values_length() + sizeof(arrow::StringArray::offset_type) * column->length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::LargeStringType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::LargeStringArray>(column);
    return typedColumn->total_values_length() + sizeof(arrow::LargeStringArray::offset_type) * column->length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::BinaryType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
    return typedColumn->total_values_length() + sizeof(arrow::BinaryArray::offset_type) * column->length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::LargeBinaryType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::LargeBinaryArray>(column);
    return typedColumn->total_values_length() + sizeof(arrow::LargeBinaryArray::offset_type) * column->length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::FixedSizeBinaryType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(column);
    return typedColumn->byte_width() * typedColumn->length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::Decimal128Type>(const std::shared_ptr<arrow::Array>& column) {
    return sizeof(ui64) * 2 * column->length();
}

ui64 GetArrayDataSize(const std::shared_ptr<arrow::Array>& column) {
    auto type = column->type();
    if (type->id() == arrow::Type::DICTIONARY) {
        auto dictArray = static_pointer_cast<arrow::DictionaryArray>(column);
        return GetDictionarySize(dictArray);
    }
    ui64 bytes = 0;
    bool success = SwitchTypeWithNull(type->id(), [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
        Y_UNUSED(typeHolder);
        bytes = GetArrayDataSizeImpl<TType>(column);
        return true;
    });

    // Add null bit mask overhead if any.
    if (HasNulls(column)) {
        bytes += column->length() / 8 + 1;
    }

    Y_DEBUG_ABORT_UNLESS(success, "Unsupported arrow type %s", type->ToString().data());
    return bytes;
}

NKikimr::NArrow::TSerializedBatch TSerializedBatch::Build(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context) {
    std::optional<TString> specialKeys;
    if (context.GetFieldsForSpecialKeys().size()) {
        specialKeys = TFirstLastSpecialKeys(batch, context.GetFieldsForSpecialKeys()).SerializeToString();
    }
    return TSerializedBatch(NArrow::SerializeSchema(*batch->schema()), NArrow::SerializeBatchNoCompression(batch), batch->num_rows(), 
        NArrow::GetBatchDataSize(batch), specialKeys);
}

TConclusionStatus TSerializedBatch::BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context, std::optional<TSerializedBatch>& sbL, std::optional<TSerializedBatch>& sbR) {
    TSerializedBatch sb = TSerializedBatch::Build(batch, context);
    const ui32 length = batch->num_rows();
    if (sb.GetSize() <= context.GetSizeLimit()) {
        sbL = std::move(sb);
        return TConclusionStatus::Success();
    } else if (length == 1) {
        return TConclusionStatus::Fail(TStringBuilder() << "original batch too big: " << sb.GetSize() << " and contains 1 row (cannot be splitted)");
    } else {
        const ui32 delta = length / 2;
        TSerializedBatch localSbL = TSerializedBatch::Build(batch->Slice(0, delta), context);
        TSerializedBatch localSbR = TSerializedBatch::Build(batch->Slice(delta, length - delta), context);
        if (localSbL.GetSize() > context.GetSizeLimit() || localSbR.GetSize() > context.GetSizeLimit()) {
            return TConclusionStatus::Fail(TStringBuilder() << "original batch too big: " << sb.GetSize() << " and after 2 parts split we have: "
                << localSbL.GetSize() << "(" << localSbL.GetRowsCount() << ")" << " / "
                << localSbR.GetSize() << "(" << localSbR.GetRowsCount() << ")" << " part sizes. Its unexpected for limit " << context.GetSizeLimit());
        }
        sbL = std::move(localSbL);
        sbR = std::move(localSbR);
        return TConclusionStatus::Success();
    }
}

TConclusion<std::vector<TSerializedBatch>> TSerializedBatch::BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const TBatchSplitttingContext& context) {
    std::vector<TSerializedBatch> result;
    std::optional<TSerializedBatch> sbL;
    std::optional<TSerializedBatch> sbR;
    auto simpleResult = TSerializedBatch::BuildWithLimit(batch, context, sbL, sbR);
    if (simpleResult.IsFail()) {
        return simpleResult;
    }
    if (sbL) {
        result.emplace_back(std::move(*sbL));
    }
    if (sbR) {
        result.emplace_back(std::move(*sbR));
    }
    return result;
}

TString TSerializedBatch::DebugString() const {
    return TStringBuilder() << "(data_size=" << Data.size() << ";schema_data_size=" << SchemaData.size() << ";rows_count=" << RowsCount << ";raw_bytes=" << RawBytes << ";)";
}

}
