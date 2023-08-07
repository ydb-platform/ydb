#include "size_calcer.h"
#include "switch_type.h"
#include "arrow_helpers.h"
#include "dictionary/conversion.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <util/system/yassert.h>

namespace NKikimr::NArrow {

TSplitBlobResult SplitByBlobSize(const std::shared_ptr<arrow::RecordBatch>& batch, const ui32 sizeLimit) {
    std::vector<TSerializedBatch> resultLocal;
    TString errorMessage;
    if (GetBatchDataSize(batch) <= sizeLimit) {
        if (!TSerializedBatch::BuildWithLimit(batch, sizeLimit, resultLocal, &errorMessage)) {
            return TSplitBlobResult("full batch splitting: " + errorMessage);
        } else {
            return TSplitBlobResult(std::move(resultLocal));
        }
    }
    TRowSizeCalculator rowCalculator(8);
    if (!rowCalculator.InitBatch(batch)) {
        return TSplitBlobResult("unexpected column type on batch initialization for row size calculator");
    }
    ui32 currentSize = 0;
    ui32 startIdx = 0;
    for (ui32 i = 0; i < batch->num_rows(); ++i) {
        const ui32 rowSize = rowCalculator.GetRowBytesSize(i);
        if (rowSize > sizeLimit) {
            return TSplitBlobResult("there is row with size more then limit (" + ::ToString(sizeLimit) + ")");
        }
        if (rowCalculator.GetApproxSerializeSize(currentSize + rowSize) > sizeLimit) {
            if (!currentSize) {
                return TSplitBlobResult("there is row with size + metadata more then limit (" + ::ToString(sizeLimit) + ")");
            }
            if (!TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, i - startIdx), sizeLimit, resultLocal, &errorMessage)) {
                return TSplitBlobResult("cannot build blobs for batch slice (" + ::ToString(i - startIdx) + " rows): " + errorMessage);
            }
            currentSize = 0;
            startIdx = i;
        }
        currentSize += rowSize;
    }
    if (currentSize) {
        if (!TSerializedBatch::BuildWithLimit(batch->Slice(startIdx, batch->num_rows() - startIdx), sizeLimit, resultLocal, &errorMessage)) {
            return TSplitBlobResult("cannot build blobs for last batch slice (" + ::ToString(batch->num_rows() - startIdx) + " rows): " + errorMessage);
        }
    }
    return TSplitBlobResult(std::move(resultLocal));
}

ui32 TRowSizeCalculator::GetRowBitWidth(const ui32 row) const {
    Y_VERIFY(Prepared);
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

ui64 GetArrayDataRawSize(const std::shared_ptr<arrow::ArrayData>& data) {
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
        bytes += GetArrayDataRawSize(column);
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

    Y_VERIFY_DEBUG(success, "Unsupported arrow type %s", type->ToString().data());
    return bytes;
}

NKikimr::NArrow::TSerializedBatch TSerializedBatch::Build(std::shared_ptr<arrow::RecordBatch> batch) {
    return TSerializedBatch(NArrow::SerializeSchema(*batch->schema()), NArrow::SerializeBatchNoCompression(batch), batch->num_rows(), NArrow::GetBatchDataSize(batch));
}

bool TSerializedBatch::BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const ui32 sizeLimit, std::optional<TSerializedBatch>& sbL, std::optional<TSerializedBatch>& sbR, TString* errorMessage) {
    TSerializedBatch sb = TSerializedBatch::Build(batch);
    const ui32 length = batch->num_rows();
    if (sb.GetSize() <= sizeLimit) {
        sbL = std::move(sb);
        return true;
    } else if (length == 1) {
        if (errorMessage) {
            *errorMessage = TStringBuilder() << "original batch too big: " << sb.GetSize() << " and contains 1 row (cannot be splitted)";
        }
        return false;
    } else {
        const ui32 delta = length / 2;
        TSerializedBatch localSbL = TSerializedBatch::Build(batch->Slice(0, delta));
        TSerializedBatch localSbR = TSerializedBatch::Build(batch->Slice(delta, length - delta));
        if (localSbL.GetSize() > sizeLimit || localSbR.GetSize() > sizeLimit) {
            if (errorMessage) {
                *errorMessage = TStringBuilder() << "original batch too big: " << sb.GetSize() << " and after 2 parts split we have: "
                    << localSbL.GetSize() << "(" << localSbL.GetRowsCount() << ")" << " / "
                    << localSbR.GetSize() << "(" << localSbR.GetRowsCount() << ")" << " part sizes. Its unexpected for limit " << sizeLimit;
            }
            return false;
        }
        sbL = std::move(localSbL);
        sbR = std::move(localSbR);
        return true;
    }
}

bool TSerializedBatch::BuildWithLimit(std::shared_ptr<arrow::RecordBatch> batch, const ui32 sizeLimit, std::vector<TSerializedBatch>& result, TString* errorMessage) {
    std::optional<TSerializedBatch> sbL;
    std::optional<TSerializedBatch> sbR;
    if (!TSerializedBatch::BuildWithLimit(batch, sizeLimit, sbL, sbR, errorMessage)) {
        return false;
    }
    if (sbL) {
        result.emplace_back(std::move(*sbL));
    }
    if (sbR) {
        result.emplace_back(std::move(*sbR));
    }
    return true;
}

}
