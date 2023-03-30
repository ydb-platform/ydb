#include "size_calcer.h"
#include "switch_type.h"
#include "arrow_helpers.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

bool SplitBySize(const std::shared_ptr<arrow::RecordBatch>& batch, std::vector<std::shared_ptr<arrow::RecordBatch>>& result, const ui32 sizeLimit) {
    if (GetBatchDataSize(batch) <= sizeLimit) {
        result = { batch };
        return true;
    }
    TRowSizeCalculator rowCalculator;
    if (!rowCalculator.InitBatch(batch)) {
        return false;
    }
    std::vector<std::shared_ptr<arrow::RecordBatch>> resultLocal;
    ui32 currentSize = 0;
    ui32 startIdx = 0;
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        const ui32 rowSize = rowCalculator.GetRowBytesSize(i);
        if (currentSize + rowSize > sizeLimit) {
            if (!currentSize) {
                return false;
            }
            resultLocal.emplace_back(batch->Slice(startIdx, i - startIdx));
            currentSize = 0;
            startIdx = i;
        }
        currentSize += rowSize;
    }
    if (currentSize) {
        resultLocal.emplace_back(batch->Slice(startIdx, batch->num_rows() - startIdx));
    }
    std::swap(resultLocal, result);
    return true;
}

ui32 TRowSizeCalculator::GetRowBitWidth(const ui32 row) const {
    Y_VERIFY(Prepared);
    ui32 result = CommonSize;
    for (auto&& c : BinaryColumns) {
        result += c->GetView(row).size() * 8;
    }
    for (auto&& c : StringColumns) {
        result += c->GetView(row).size() * 8;
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
            CommonSize += fSize->bit_width();
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

ui64 GetBatchDataSize(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return 0;
    }
    ui64 bytes = 0;
    for (auto& column : batch->columns()) { // TODO: use column_data() instead of columns()
        bytes += GetArrayDataSize(column);
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
    return typedColumn->total_values_length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::LargeStringType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::StringArray>(column);
    return typedColumn->total_values_length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::BinaryType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
    return typedColumn->total_values_length();
}

template <>
ui64 GetArrayDataSizeImpl<arrow::LargeBinaryType>(const std::shared_ptr<arrow::Array>& column) {
    auto typedColumn = std::static_pointer_cast<arrow::BinaryArray>(column);
    return typedColumn->total_values_length();
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

    Y_VERIFY_DEBUG(success, "Unsupported arrow type %s", type->ToString().data());
    return bytes;
}

}
