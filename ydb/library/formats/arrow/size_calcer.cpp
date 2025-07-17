#include "size_calcer.h"
#include "arrow_helpers.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/visitor_inline.h>
#include <util/system/yassert.h>
#include <util/string/builder.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NArrow {

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

ui64 GetArrayMemorySize(const std::shared_ptr<arrow::ChunkedArray>& data) {
    if (!data) {
        return 0;
    }
    ui64 result = 0;
    for (auto&& i : data->chunks()) {
        result += GetArrayMemorySize(i->data());
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
    return GetBatchDataSize(batch->columns());
}

ui64 GetBatchMemorySize(const std::shared_ptr<arrow::RecordBatch>& batch) {
    if (!batch) {
        return 0;
    }
    return GetBatchMemorySize(batch->columns());
}

ui64 GetBatchDataSize(const std::vector<std::shared_ptr<arrow::Array>>& columns) {
    ui64 bytes = 0;
    for (auto& column : columns) {
        bytes += GetArrayDataSize(column);
    }
    return bytes;
}

ui64 GetBatchMemorySize(const std::vector<std::shared_ptr<arrow::Array>>& columns) {
    ui64 bytes = 0;
    for (auto& column : columns) {
        bytes += GetArrayMemorySize(column->data());
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

namespace {

class TSizeVisitor {
    const std::shared_ptr<arrow::Array>& Column;

    YDB_READONLY_DEF(ui64, Bytes);


public:
    explicit TSizeVisitor(const std::shared_ptr<arrow::Array>& column)
        : Column(column)
    {}

    template <typename TType>
    arrow::Status Visit(const TType& type) {
        return arrow::Status::NotImplemented(TStringBuilder() << "unsupported arrow type " << type.ToString());
    }

    template <typename TType>
        requires arrow::has_c_type<TType>::value
    arrow::Status Visit(const TType&) {
        Bytes += sizeof(typename TType::c_type) * Column->length();
        return arrow::Status::OK();
    }

    template <>
    arrow::Status Visit(const arrow::NullType&) {
        Bytes += Column->length() * 8; // Special value for empty lines
        return arrow::Status::OK();
    }

    template <typename TType>
        requires arrow::is_base_binary_type<TType>::value
    arrow::Status Visit(const TType&) {
        using TArray = typename arrow::TypeTraits<TType>::ArrayType;

        auto typedColumn = std::static_pointer_cast<TArray>(Column);
        Bytes += typedColumn->total_values_length() + sizeof(typename TArray::offset_type) * Column->length();
        return arrow::Status::OK();
    }

    template <typename TType>
        requires arrow::is_fixed_size_binary_type<TType>::value
    arrow::Status Visit(const TType&) {
        using TArray = typename arrow::TypeTraits<TType>::ArrayType;

        auto typedColumn = std::static_pointer_cast<TArray>(Column);
        Bytes += typedColumn->byte_width() * typedColumn->length();
        return arrow::Status::OK();
    }

    template <>
    arrow::Status Visit(const arrow::FixedSizeListType&) {
        auto typedColumn = std::static_pointer_cast<arrow::FixedSizeListArray>(Column);
        auto offset = typedColumn->value_offset(0);
        auto length = typedColumn->value_length() * typedColumn->length();
        Bytes += GetArrayDataSize(typedColumn->values()->Slice(offset, length));
        return arrow::Status::OK();
    }

    template <typename TType>
        requires arrow::is_var_length_list_type<TType>::value
    arrow::Status Visit(const TType&) {
        using TArray = typename arrow::TypeTraits<TType>::ArrayType;

        auto typedColumn = std::static_pointer_cast<TArray>(Column);
        auto numberElements = typedColumn->length();
        if (numberElements <= 0) {
            return arrow::Status::OK();
        }

        auto offset = typedColumn->value_offset(0);
        auto length = typedColumn->value_offset(numberElements - 1) + typedColumn->value_length(numberElements - 1) - offset;
        Bytes += GetArrayDataSize(typedColumn->values()->Slice(offset, length)) + sizeof(typename TArray::offset_type) * numberElements;
        return arrow::Status::OK();
    }

    template <>
    arrow::Status Visit(const arrow::StructType&) {
        auto typedColumn = std::static_pointer_cast<arrow::StructArray>(Column);
        for (const auto& field : typedColumn->fields()) {
            Bytes += GetArrayDataSize(field);
        }
        return arrow::Status::OK();
    }

    template <>
    arrow::Status Visit(const arrow::SparseUnionType&) {
        auto typedColumn = std::static_pointer_cast<arrow::SparseUnionArray>(Column);
        Bytes += sizeof(typename arrow::SparseUnionArray::type_code_t) * typedColumn->length();
        for (int fieldId = 0; fieldId < typedColumn->union_type()->num_fields(); ++fieldId) {
            Bytes += GetArrayDataSize(typedColumn->field(fieldId));
        }
        return arrow::Status::OK();
    }
};

}

ui64 GetArrayDataSize(const std::shared_ptr<arrow::Array>& column) {
    auto typeId = column->type_id();
    if (typeId == arrow::Type::DICTIONARY) {
        auto dictArray = static_pointer_cast<arrow::DictionaryArray>(column);
        return GetDictionarySize(dictArray);
    }

    TSizeVisitor visitor(column);
    auto status = arrow::VisitTypeInline(*type, &visitor);
    Y_VERIFY_S(status.ok(), "Failed to calculate array size: " << status.ToString());

    ui64 bytes = visitor.GetBytes();

    // Add null bit mask overhead if any.
    if (HasNulls(column)) {
        bytes += column->length() / 8 + 1;
    }

    return bytes;
}

ui64 GetDictionarySize(const std::shared_ptr<arrow::DictionaryArray>& data) {
    if (!data) {
        return 0;
    }
    return GetArrayDataSize(data->dictionary()) + GetArrayDataSize(data->indices());
}

}
