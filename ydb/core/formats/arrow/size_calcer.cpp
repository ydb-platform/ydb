#include "size_calcer.h"
#include "switch_type.h"
#include "arrow_helpers.h"
#include "dictionary/conversion.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/yassert.h>
#include <util/string/builder.h>

namespace NKikimr::NArrow {

TConclusion<std::vector<TSerializedBatch>> SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const TBatchSplitttingContext& context) {
    if (GetBatchDataSize(batch) <= context.GetSizeLimit()) {
        return TSerializedBatch::BuildWithLimit(batch, context);
    }
    TRowSizeCalculator rowCalculator(8);
    if (!rowCalculator.InitBatch(batch)) {
        return TConclusionStatus::Fail("unexpected column type on batch initialization for row size calculator");
    }
    ui32 currentSize = 0;
    ui32 startIdx = 0;
    std::vector<TSerializedBatch> result;
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        const ui32 rowSize = rowCalculator.GetRowBytesSize(i);
        if (rowSize > context.GetSizeLimit()) {
            return TConclusionStatus::Fail("there is row with size more then limit (" + ::ToString(context.GetSizeLimit()) + ")");
        }
        if (rowCalculator.GetApproxSerializeSize(currentSize + rowSize) > context.GetSizeLimit()) {
            if (!currentSize) {
                return TConclusionStatus::Fail("there is row with size + metadata more then limit (" + ::ToString(context.GetSizeLimit()) + ")");
            }
            auto localResult = TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, i - startIdx), context);
            if (localResult.IsFail()) {
                return TConclusionStatus::Fail("cannot build blobs for batch slice (" + ::ToString(i - startIdx) + " rows): " + localResult.GetErrorMessage());
            } else {
                result.insert(result.end(), localResult.GetResult().begin(), localResult.GetResult().end());
            }
            currentSize = 0;
            startIdx = i;
        }
        currentSize += rowSize;
    }
    if (currentSize) {
        auto localResult = TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, batch->num_rows() - startIdx), context);
        if (localResult.IsFail()) {
            return TConclusionStatus::Fail("cannot build blobs for last batch slice (" + ::ToString(batch->num_rows() - startIdx) + " rows): " + localResult.GetErrorMessage());
        } else {
            result.insert(result.end(), localResult.GetResult().begin(), localResult.GetResult().end());
        }
    }
    return result;
}

ui32 TRowSizeCalculator::GetRowBitWidth(const ui32 row) const {
    Y_ABORT_UNLESS(Prepared);
    ui32 result = CommonSize;
    for (auto&& c : BinaryColumns) {
        result += GetBitWidthAligned(c->GetView(row).size() * 8);
    }
    for (auto&& c : StringColumns) {
        result += GetBitWidthAligned(c->GetView(row).size() * 8);
    }
    return result;
}

bool TRowSizeCalculator::InitBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    Batch = batch;
    CommonSize = 0;
    BinaryColumns.clear();
    StringColumns.clear();
    Prepared = false;
    for (ui32 i = 0; i < (ui32)Batch->num_columns(); ++i) {
        auto fSize = std::dynamic_pointer_cast<arrow::FixedWidthType>(Batch->column(i)->type());
        if (fSize) {
            CommonSize += GetBitWidthAligned(fSize->bit_width());
        } else {
            auto c = Batch->column(i);
            if (c->type()->id() == arrow::Type::BINARY) {
                const arrow::BinaryArray& viewArray = static_cast<const arrow::BinaryArray&>(*c);
                BinaryColumns.emplace_back(&viewArray);
            } else if (c->type()->id() == arrow::Type::STRING) {
                const arrow::StringArray& viewArray = static_cast<const arrow::StringArray&>(*c);
                StringColumns.emplace_back(&viewArray);
            } else {
                return false;
            }
        }
    }
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
