#include "converter.h"
#include "switch_type.h"

#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>

#include <util/memory/pool.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

#include <memory>
#include <vector>

namespace NKikimr::NArrow {

static bool ConvertData(TCell& cell, const NScheme::TTypeInfo& colType, TMemoryPool& memPool, TString& errorMessage) {
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
            const auto binaryJson = NBinaryJson::SerializeToBinaryJson(cell.AsBuf());
            if (!binaryJson.Defined()) {
                errorMessage = "Invalid JSON for JsonDocument provided";
                return false;
            }
            const auto saved = memPool.AppendString(TStringBuf(binaryJson->Data(), binaryJson->Size()));
            cell = TCell(saved.data(), saved.size());
            break;
        }
        case NScheme::NTypeIds::Decimal:
            errorMessage = "Decimal conversion is not supported yet";
            return false;
        default:
            break;
    }
    return true;
}

static bool ConvertColumn(const NScheme::TTypeInfo colType, std::shared_ptr<arrow::Array>& column, std::shared_ptr<arrow::Field>& field) {
    if (colType.GetTypeId() == NScheme::NTypeIds::Decimal) {
        return false;
    }

    if ((colType.GetTypeId() == NScheme::NTypeIds::JsonDocument) &&
        (column->type()->id() == arrow::Type::BINARY || column->type()->id() == arrow::Type::STRING))
    {
        ;
    } else if (column->type()->id() != arrow::Type::BINARY) {
        return false;
    }

    auto& binaryArray = static_cast<arrow::BinaryArray&>(*column);
    arrow::BinaryBuilder builder;
    builder.Reserve(binaryArray.length()).ok();
    // TODO: ReserveData

    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber: {
            for (i32 i = 0; i < binaryArray.length(); ++i) {
                auto value = binaryArray.Value(i);
                const auto dyNumber = NDyNumber::ParseDyNumberString(TStringBuf(value.data(), value.size()));
                if (!dyNumber.Defined() || !builder.Append((*dyNumber).data(), (*dyNumber).size()).ok()) {
                    return false;
                }
            }
        }
        case NScheme::NTypeIds::JsonDocument: {
            for (i32 i = 0; i < binaryArray.length(); ++i) {
                auto value = binaryArray.Value(i);
                if (!value.size()) {
                    Y_VERIFY(builder.AppendNull().ok());
                    continue;
                }
                const auto binaryJson = NBinaryJson::SerializeToBinaryJson(TStringBuf(value.data(), value.size()));
                if (!binaryJson.Defined() || !builder.Append(binaryJson->Data(), binaryJson->Size()).ok()) {
                    return false;
                }
            }
        }
        default:
            break;
    }

    std::shared_ptr<arrow::BinaryArray> result;
    if (!builder.Finish(&result).ok()) {
        return false;
    }

    column = result;
    if (colType.GetTypeId() == NScheme::NTypeIds::JsonDocument && field->type()->id() == arrow::Type::STRING) {
        field = std::make_shared<arrow::Field>(field->name(), std::make_shared<arrow::BinaryType>());
    }

    return true;
}

std::shared_ptr<arrow::RecordBatch> ConvertColumns(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                   const THashMap<TString, NScheme::TTypeInfo>& columnsToConvert)
{
    std::vector<std::shared_ptr<arrow::Array>> columns = batch->columns();
    std::vector<std::shared_ptr<arrow::Field>> fields = batch->schema()->fields();
    Y_VERIFY(columns.size() == fields.size());
    for (i32 i = 0; i < batch->num_columns(); ++i) {
        auto& colName = batch->column_name(i);
        auto it = columnsToConvert.find(TString(colName.data(), colName.size()));
        if (it != columnsToConvert.end()) {
            if (!ConvertColumn(it->second, columns[i], fields[i])) {
                return {};
            }
        }
    }
    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(std::move(fields)), batch->num_rows(), std::move(columns));
}

static std::shared_ptr<arrow::Array> InplaceConvertColumn(const std::shared_ptr<arrow::Array>& column,
                                                   NScheme::TTypeInfo colType) {
    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::Bytes: {
            Y_VERIFY(column->type()->id() == arrow::Type::STRING);
            return std::make_shared<arrow::BinaryArray>(column->data());
        }
        case NScheme::NTypeIds::Date: {
            Y_VERIFY(arrow::is_primitive(column->type()->id()));
            Y_VERIFY(arrow::bit_width(column->type()->id()) == 16);
            return std::make_shared<arrow::NumericArray<arrow::UInt16Type>>(column->data());
        }
        case NScheme::NTypeIds::Datetime: {
            Y_VERIFY(arrow::is_primitive(column->type()->id()));
            Y_VERIFY(arrow::bit_width(column->type()->id()) == 32);
            return std::make_shared<arrow::NumericArray<arrow::Int32Type>>(column->data());
        }
        case NScheme::NTypeIds::Timestamp: {
            Y_VERIFY(arrow::is_primitive(column->type()->id()));
            Y_VERIFY(arrow::bit_width(column->type()->id()) == 64);
            return std::make_shared<arrow::TimestampArray>(column->data());
        }
        default:
            return {};
    }
}

std::shared_ptr<arrow::RecordBatch> InplaceConvertColumns(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                          const THashMap<TString, NScheme::TTypeInfo>& columnsToConvert) {
    std::vector<std::shared_ptr<arrow::Array>> columns = batch->columns();
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(batch->num_columns());
    for (i32 i = 0; i < batch->num_columns(); ++i) {
        auto& colName = batch->column_name(i);
        auto it = columnsToConvert.find(TString(colName.data(), colName.size()));
        if (it != columnsToConvert.end()) {
            columns[i] = InplaceConvertColumn(columns[i], it->second);
        }
        fields.push_back(std::make_shared<arrow::Field>(colName, columns[i]->type()));
    }
    auto resultSchemaFixed = std::make_shared<arrow::Schema>(std::move(fields));
    auto convertedBatch = arrow::RecordBatch::Make(resultSchemaFixed, batch->num_rows(), std::move(columns));
    Y_VERIFY(convertedBatch->ValidateFull() == arrow::Status::OK());
    return convertedBatch;
}

bool TArrowToYdbConverter::NeedDataConversion(const NScheme::TTypeInfo& colType) {
    switch (colType.GetTypeId()) {
        case NScheme::NTypeIds::DyNumber:
        case NScheme::NTypeIds::JsonDocument:
        case NScheme::NTypeIds::Decimal:
            return true;
        default:
            break;
    }
    return false;
}

bool TArrowToYdbConverter::NeedInplaceConversion(const NScheme::TTypeInfo& typeInRequest, const NScheme::TTypeInfo& expectedType) {
    switch (expectedType.GetTypeId()) {
        case NScheme::NTypeIds::Bytes:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Utf8;
        case NScheme::NTypeIds::Date:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Uint16;
        case NScheme::NTypeIds::Datetime:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Int32;
        case NScheme::NTypeIds::Timestamp:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Int64;
        default:
            break;
    }
    return false;
}

bool TArrowToYdbConverter::NeedConversion(const NScheme::TTypeInfo& typeInRequest, const NScheme::TTypeInfo& expectedType) {
    switch (expectedType.GetTypeId()) {
        case NScheme::NTypeIds::JsonDocument:
            return typeInRequest.GetTypeId() == NScheme::NTypeIds::Utf8;
        default:
            break;
    }
    return false;
}

bool TArrowToYdbConverter::Process(const arrow::RecordBatch& batch, TString& errorMessage) {
    std::vector<std::shared_ptr<arrow::Array>> allColumns;
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
            // TODO: support pg types
            Y_VERIFY(colType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");

            auto& column = allColumns[col];
            bool success = SwitchYqlTypeToArrowType(colType, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                for (i32 i = 0; i < unroll; ++i) {
                    i32 realRow = row + i;
                    if (column->IsNull(realRow)) {
                        cells[i][col] = TCell();
                    } else {
                        cells[i][col] = MakeCell<typename arrow::TypeTraits<TType>::ArrayType>(column, realRow);
                    }
                }
                return true;
            });

            if (!success) {
                errorMessage = TStringBuilder() << "No arrow conversion for type Yql::" << NScheme::TypeName(colType.GetTypeId())
                        << " at column '" << colName << "'";
                return false;
            }

            if (NeedDataConversion(colType)) {
                memPool.Clear();
                for (i32 i = 0; i < unroll; ++i) {
                    if (!ConvertData(cells[i][col], colType, memPool, errorMessage)) {
                        return false;
                    }
                }
            }

            ++col;
        }

        for (i32 i = 0; i < unroll; ++i) {
            RowWriter_.AddRow(cells[i]);
        }
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
            // TODO: support pg types
            Y_VERIFY(colType.GetTypeId() != NScheme::NTypeIds::Pg, "pg types are not supported");

            auto& column = allColumns[col];
            auto& curCell = cells[0][col];
            if (column->IsNull(row)) {
                curCell = TCell();
                ++col;
                continue;
            }

            bool success = SwitchYqlTypeToArrowType(colType, [&]<typename TType>(TTypeWrapper<TType> typeHolder) {
                Y_UNUSED(typeHolder);
                curCell = MakeCell<typename arrow::TypeTraits<TType>::ArrayType>(column, row);
                return true;
            });

            if (!success) {
                errorMessage = TStringBuilder() << "No arrow conversion for type Yql::" << NScheme::TypeName(colType.GetTypeId())
                        << " at column '" << colName << "'";
                return false;
            }

            if (!ConvertData(curCell, colType, memPool, errorMessage)) {
                return false;
            }
            ++col;
        }

        RowWriter_.AddRow(cells[0]);
    }

    return true;
}

} // namespace NKikimr::NArrow
