#include "converter.h"
#include "switch/switch_type.h"

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/types/dynumber/dynumber.h>

#include <util/generic/set.h>
#include <util/memory/pool.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>

#include <memory>
#include <vector>

namespace NKikimr::NArrow {

static bool ConvertData(TCell& cell, const NScheme::TTypeInfo& colType, TMemoryPool& memPool, TString& errorMessage, const bool allowInfDouble) {
    if (!cell.AsBuf()) {
        cell = TCell();
        return true;
    }
    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber: {
            const auto dyNumber = NDyNumber::ParseDyNumberString(cell.AsBuf());
            if (!dyNumber.Defined()) {
                errorMessage = "Invalid DyNumber string representation";
                return false;
            }
            const auto dyNumberInPool = memPool.AppendString(TStringBuf(*dyNumber));
            cell = TCell(dyNumberInPool.data(), dyNumberInPool.size());
            break;
        }
        case NScheme::NTypeIds::JsonDocument: {
            const auto binaryJson = NBinaryJson::SerializeToBinaryJson(cell.AsBuf(), allowInfDouble);
            if (std::holds_alternative<TString>(binaryJson)) {
                errorMessage = "Invalid JSON for JsonDocument provided: " + std::get<TString>(binaryJson);
                return false;
            }
            const auto& value = std::get<NBinaryJson::TBinaryJson>(binaryJson);
            const auto saved = memPool.AppendString(TStringBuf(value.Data(), value.Size()));
            cell = TCell(saved.data(), saved.size());
            break;
        }
        default:
            break;
    }
    return true;
}

static arrow20::Status ConvertColumn(
    const NScheme::TTypeInfo colType, std::shared_ptr<arrow20::Array>& column, std::shared_ptr<arrow20::Field>& field, const bool allowInfDouble) {
    switch (colType.GetTypeId()) {
    case NScheme::NTypeIds::DyNumber: {
        if (!arrow20::is_binary_like(column->type()->id())) {
            return arrow20::Status::TypeError("Cannot convert DyNumber to ", column->type()->ToString());
        }

        if (field->type()->id() == arrow20::Type::STRING) {
            field = std::make_shared<arrow20::Field>(field->name(), std::make_shared<arrow20::BinaryType>(), field->nullable());
        }
        break;
    }
    case NScheme::NTypeIds::JsonDocument: {
        if (!arrow20::is_binary_like(column->type()->id())) {
            return arrow20::Status::TypeError("Cannot convert JsonDocument to ", column->type()->ToString());
        }

        if (field->type()->id() == arrow20::Type::STRING) {
            field = std::make_shared<arrow20::Field>(field->name(), std::make_shared<arrow20::BinaryType>(), field->nullable());
        }
        break;
    }
    default:
        if (column->type()->id() != arrow20::Type::BINARY) {
            return arrow20::Status::TypeError("Cannot convert ", NScheme::TypeName(colType), " to ", column->type()->ToString());
        }
    }

    auto& binaryArray = static_cast<arrow20::BinaryArray&>(*column);
    arrow20::BinaryBuilder builder;
    builder.Reserve(binaryArray.length()).ok();
    // TODO: ReserveData

    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber: {
            for (i32 i = 0; i < binaryArray.length(); ++i) {
                auto value = binaryArray.Value(i);
                const auto dyNumber = NDyNumber::ParseDyNumberString(TStringBuf(value.data(), value.size()));
                if (!dyNumber.Defined()) {
                    return arrow20::Status::SerializationError("Cannot parse dy number: ", value);
                }
                auto appendResult = builder.Append((*dyNumber).data(), (*dyNumber).size());
                if (!appendResult.ok()) {
                    return appendResult;
                }
            }
	    break;
        }
        case NScheme::NTypeIds::JsonDocument: {
            for (i32 i = 0; i < binaryArray.length(); ++i) {
                auto value = binaryArray.Value(i);
                if (!value.size()) {
                    Y_ABORT_UNLESS(builder.AppendNull().ok());
                    continue;
                }
                const TStringBuf valueBuf(value.data(), value.size());
                if (NBinaryJson::IsValidBinaryJson(valueBuf)) {
                    auto appendResult = builder.Append(value);
                    if (!appendResult.ok()) {
                        return appendResult;
                    }
                } else {
                    const auto maybeBinaryJson = NBinaryJson::SerializeToBinaryJson(valueBuf, allowInfDouble);
                    if (std::holds_alternative<TString>(maybeBinaryJson)) {
                        return arrow20::Status::SerializationError("Cannot serialize json (", std::get<TString>(maybeBinaryJson),
                            "): ", valueBuf.SubStr(0, Min(valueBuf.Size(), size_t{1024})));
                    }
                    const auto& binaryJson = std::get<NBinaryJson::TBinaryJson>(maybeBinaryJson);
                    auto appendResult = builder.Append(binaryJson.Data(), binaryJson.Size());
                    if (!appendResult.ok()) {
                        return appendResult;
                    }
                }
            }
	    break;
        }
        default:
            break;
    }

    std::shared_ptr<arrow20::BinaryArray> result;
    auto finishResult = builder.Finish(&result);
    if (!finishResult.ok()) {
        return finishResult;
    }

    column = result;
    return arrow20::Status::OK();
}

arrow20::Result<std::shared_ptr<arrow20::RecordBatch>> ConvertColumns(
    const std::shared_ptr<arrow20::RecordBatch>& batch, const THashMap<TString, NScheme::TTypeInfo>& columnsToConvert, const bool allowInfDouble) {
    std::vector<std::shared_ptr<arrow20::Array>> columns = batch->columns();
    std::vector<std::shared_ptr<arrow20::Field>> fields = batch->schema()->fields();
    Y_ABORT_UNLESS(columns.size() == fields.size());

    for (i32 i = 0; i < batch->num_columns(); ++i) {
        auto& colName = batch->column_name(i);
        auto it = columnsToConvert.find(TString(colName.data(), colName.size()));
        if (it != columnsToConvert.end()) {
            auto convertResult = ConvertColumn(it->second, columns[i], fields[i], allowInfDouble);
            if (!convertResult.ok()) {
                return arrow20::Status::FromArgs(convertResult.code(), "column ", colName, ": ", convertResult.ToString());
            }
        }
    }
    return arrow20::RecordBatch::Make(std::make_shared<arrow20::Schema>(std::move(fields)), batch->num_rows(), std::move(columns));
}

static std::shared_ptr<arrow20::Array> InplaceConvertColumn(const std::shared_ptr<arrow20::Array>& column,
                                                   NScheme::TTypeInfo colType) {
    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::Bytes: {
            Y_ABORT_UNLESS(column->type()->id() == arrow20::Type::STRING);
            return std::make_shared<arrow20::BinaryArray>(
                arrow20::ArrayData::Make(arrow20::binary(), column->data()->length,
                    column->data()->buffers, column->data()->null_count, column->data()->offset));
        }
        case NScheme::NTypeIds::Date: {
            Y_ABORT_UNLESS(arrow20::is_primitive(column->type()->id()));
            Y_ABORT_UNLESS(arrow20::bit_width(column->type()->id()) == 16);

            auto newData = column->data()->Copy();
            newData->type = arrow20::uint16();
            return std::make_shared<arrow20::NumericArray<arrow20::UInt16Type>>(newData);
        }
        case NScheme::NTypeIds::Datetime: {
            Y_ABORT_UNLESS(arrow20::is_primitive(column->type()->id()));
            Y_ABORT_UNLESS(arrow20::bit_width(column->type()->id()) == 32);

            auto newData = column->data()->Copy();
            newData->type = arrow20::uint32();
            return std::make_shared<arrow20::NumericArray<arrow20::UInt32Type>>(newData);
        }
        case NScheme::NTypeIds::Timestamp: {
            Y_ABORT_UNLESS(arrow20::is_primitive(column->type()->id()));
            Y_ABORT_UNLESS(arrow20::bit_width(column->type()->id()) == 64);

            auto newData = column->data()->Copy();
            newData->type = arrow20::timestamp(arrow20::TimeUnit::MICRO);
            return std::make_shared<arrow20::TimestampArray>(newData);
        }
        case NScheme::NTypeIds::Date32: {
            Y_ABORT_UNLESS(arrow20::bit_width(column->type()->id()) == 32);

            auto newData = column->data()->Copy();
            newData->type = arrow20::int32();
            return std::make_shared<arrow20::NumericArray<arrow20::Int32Type>>(newData);
        }
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
        case NScheme::NTypeIds::Datetime64: {
            Y_ABORT_UNLESS(arrow20::bit_width(column->type()->id()) == 64);

            auto newData = column->data()->Copy();
            newData->type = arrow20::int64();
            return std::make_shared<arrow20::NumericArray<arrow20::Int64Type>>(newData);
        }
        default:
            return {};
    }
}

arrow20::Result<std::shared_ptr<arrow20::RecordBatch>> InplaceConvertColumns(const std::shared_ptr<arrow20::RecordBatch>& batch,
                                                          const THashMap<TString, NScheme::TTypeInfo>& columnsToConvert) {
    std::vector<std::shared_ptr<arrow20::Array>> columns = batch->columns();
    std::vector<std::shared_ptr<arrow20::Field>> fields;
    fields.reserve(batch->num_columns());

    for (i32 i = 0; i < batch->num_columns(); ++i) {
        auto& colName = batch->column_name(i);
        auto origType = batch->schema()->GetFieldByName(colName);
        auto it = columnsToConvert.find(TString(colName.data(), colName.size()));
        if (it != columnsToConvert.end()) {
            columns[i] = InplaceConvertColumn(columns[i], it->second);
        }
        fields.push_back(std::make_shared<arrow20::Field>(colName, columns[i]->type(), origType->nullable()));
    }
    auto resultSchemaFixed = std::make_shared<arrow20::Schema>(std::move(fields));
    auto convertedBatch = arrow20::RecordBatch::Make(resultSchemaFixed, batch->num_rows(), std::move(columns));

    Y_ABORT_UNLESS(convertedBatch->Validate().ok());
    Y_DEBUG_ABORT_UNLESS(convertedBatch->ValidateFull().ok());
    return convertedBatch;
}

bool TArrowToYdbConverter::NeedDataConversion(const NScheme::TTypeInfo& colType) {
    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::JsonDocument:
            return true;
        default:
            break;
    }
    return false;
}

bool TArrowToYdbConverter::NeedDataConversionWithSettings(const NScheme::TTypeInfo& colType) {
    return WithConversion_ ? NeedDataConversion(colType) : false;
}

bool TArrowToYdbConverter::NeedInplaceConversion(const NScheme::TTypeInfo& typeInRequest, const NScheme::TTypeInfo& expectedType) {
    switch (expectedType.GetTypeId()) {
        case NScheme::NTypeIds::Bytes:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Utf8;
        case NScheme::NTypeIds::Date:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Uint16;
        case NScheme::NTypeIds::Date32:
        case NScheme::NTypeIds::Datetime:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Int32;
        case NScheme::NTypeIds::Timestamp:
            if (typeInRequest.GetTypeId() == NScheme::NTypeIds::Uint64) {
                return true;
            }
            [[fallthrough]];
        case NScheme::NTypeIds::Timestamp64:
        case NScheme::NTypeIds::Interval64:
        case NScheme::NTypeIds::Datetime64:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Int64;
        default:
            break;
    }
    return false;
}

bool TArrowToYdbConverter::NeedConversion(const NScheme::TTypeInfo& typeInRequest, const NScheme::TTypeInfo& expectedType) {
    switch (expectedType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Utf8;
        case NScheme::NTypeIds::JsonDocument:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Utf8;
        default:
            break;
    }
    return false;
}

bool TArrowToYdbConverter::Process(const arrow20::RecordBatch& batch, TString& errorMessage) {
    std::vector<std::shared_ptr<arrow20::Array>> allColumns;
    allColumns.reserve(YdbSchema_.size());

    // Shrink and reorder columns
    for (auto& [colName, colType] : YdbSchema_) {
        auto column = batch.GetColumnByName(colName);
        if (!column) {
            errorMessage = TStringBuilder() << "No column '" << colName << "' in source batch";
            return false;
        }
        allColumns.emplace_back(std::move(column));
    }

    std::vector<TSmallVec<TCell>> cells;
    i64 row = 0;

    TMemoryPool memPool(256); // for convertions

#if 1 // optimization
    static constexpr i32 unroll = 32;
    cells.reserve(unroll);
    for (i32 i = 0; i < unroll; ++i) {
        cells.push_back(TSmallVec<TCell>(YdbSchema_.size()));
    }

    i64 rowsUnroll = batch.num_rows() - batch.num_rows() % unroll;
    for (; row < rowsUnroll; row += unroll) {
        ui32 col = 0;
        for (auto& [colName, colType] : YdbSchema_) {
            auto& column = allColumns[col];
            bool success = SwitchYqlTypeToArrowType(colType, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                for (i32 i = 0; i < unroll; ++i) {
                    i32 realRow = row + i;
                    if (column->IsNull(realRow)) {
                        cells[i][col] = TCell();
                    } else {
                        cells[i][col] = MakeCell<typename arrow20::TypeTraits<TType>::ArrayType>(column, realRow);
                    }
                }
                return true;
            });

            if (!success) {
                errorMessage = TStringBuilder() << "No arrow conversion for type Yql::" << NScheme::TypeName(colType.GetTypeId())
                        << " at column '" << colName << "'";
                return false;
            }

            if (NeedDataConversionWithSettings(colType)) {
                for (i32 i = 0; i < unroll; ++i) {
                    if (!ConvertData(cells[i][col], colType, memPool, errorMessage, AllowInfDouble_)) {
                        return false;
                    }
                }
            }

            ++col;
        }

        for (i32 i = 0; i < unroll; ++i) {
            RowWriter_.AddRow(cells[i]);
        }
        memPool.Clear();
    }
    cells.resize(1);
#else
    cells.reserve(1);
    cells.push_back(TSmallVec<TCell>(YdbSchema.size()));
#endif

    for (; row < batch.num_rows(); ++row) {
        memPool.Clear();

        ui32 col = 0;
        for (auto& [colName, colType] : YdbSchema_) {
            auto& column = allColumns[col];
            auto& curCell = cells[0][col];
            if (column->IsNull(row)) {
                curCell = TCell();
                ++col;
                continue;
            }

            bool success = SwitchYqlTypeToArrowType(colType, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                curCell = MakeCell<typename arrow20::TypeTraits<TType>::ArrayType>(column, row);
                return true;
            });

            if (!success) {
                errorMessage = TStringBuilder() << "No arrow conversion for type Yql::" << NScheme::TypeName(colType.GetTypeId())
                        << " at column '" << colName << "'";
                return false;
            }

            if (WithConversion_ && !ConvertData(curCell, colType, memPool, errorMessage, AllowInfDouble_)) {
                return false;
            }
            ++col;
        }

        RowWriter_.AddRow(cells[0]);
    }

    return true;
}

} // namespace NKikimr::NArrow
