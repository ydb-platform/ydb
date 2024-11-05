#include "column_converter.h"

#include "boolean_column_converter.h"
#include "floating_point_column_converter.h"
#include "integer_column_converter.h"
#include "null_column_converter.h"
#include "string_column_converter.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NColumnConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateColumnConvert(
    const TColumnSchema& columnSchema,
    int columnId,
    int columnOffset)
{
    switch (columnSchema.GetWireType()) {
        case EValueType::Int64:
            return CreateInt64ColumnConverter(columnId, columnSchema, columnOffset);

        case EValueType::Uint64:
            return CreateUint64ColumnConverter(columnId, columnSchema, columnOffset);

        case EValueType::Double:
            switch (columnSchema.CastToV1Type()) {
                case ESimpleLogicalValueType::Float:
                    return CreateFloatingPoint32ColumnConverter(columnId, columnSchema, columnOffset);
                default:
                    return CreateFloatingPoint64ColumnConverter(columnId, columnSchema, columnOffset);
            }

        case EValueType::String:
            return CreateStringConverter(columnId, columnSchema, columnOffset);

        case EValueType::Boolean:
            return CreateBooleanColumnConverter(columnId, columnSchema, columnOffset);

        case EValueType::Any:
            return CreateAnyConverter(columnId, columnSchema, columnOffset);

        case EValueType::Composite:
            return CreateCompositeConverter(columnId, columnSchema, columnOffset);

        case EValueType::Null:
            return CreateNullConverter(columnId);

        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Max:
            break;
    }
    ThrowUnexpectedValueType(columnSchema.GetWireType());
}

////////////////////////////////////////////////////////////////////////////////


TConvertedColumnRange TColumnConverters::ConvertRowsToColumns(
    TRange<TUnversionedRow> rows,
    const THashMap<int, TColumnSchema>& columnSchema)
{
    TConvertedColumnRange convertedColumnsRange;
    if (rows.size() == 0) {
        return convertedColumnsRange;
    }

    if(IsFirstBatch_) {
        // Initialize mapping column ids to indexes, since, for example, in the case of column specification (//path/to/table{column1,column3}), not all column ids will exist

        auto firstRow = rows[0];

        for (const auto* item = firstRow.Begin(); item != firstRow.End(); ++item) {
            IdsToIndexes_[item->Id] = std::ssize(ColumnIds_);
            ColumnIds_.push_back(item->Id);
            auto iterSchema = columnSchema.find(item->Id);
            if (iterSchema == columnSchema.end()) {
                THROW_ERROR_EXCEPTION("Column with Id %v has no schema", item->Id);
            }
        }
    }
    IsFirstBatch_ = false;

    std::vector<TUnversionedRowValues> rowsValues;
    rowsValues.reserve(rows.size());

    for (auto row : rows) {
        TUnversionedRowValues rowValues(ColumnIds_.size(), nullptr);
        for (const auto* item = row.Begin(); item != row.End(); ++item) {
            auto iter = IdsToIndexes_.find(item->Id);
            if(iter == IdsToIndexes_.end()) {
                THROW_ERROR_EXCEPTION("Column with Id %v has no schema", item->Id);
            }
            rowValues[iter->second] = item;
        }
        rowsValues.push_back(std::move(rowValues));
    }

    for (int offset = 0; offset < std::ssize(ColumnIds_); offset++) {
        auto iterSchema = columnSchema.find(ColumnIds_[offset]);
        YT_VERIFY(iterSchema != columnSchema.end());
        auto converter = CreateColumnConvert(iterSchema->second, ColumnIds_[offset], offset);
        auto columns = converter->Convert(rowsValues);
        convertedColumnsRange.push_back(std::move(columns));
    }
    return convertedColumnsRange;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnConverters
