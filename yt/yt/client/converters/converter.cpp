#include "converter.h"

#include "boolean_converter.h"
#include "floating_point_converter.h"
#include "integer_converter.h"
#include "null_converter.h"
#include "string_converter.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NConverters {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

IColumnConverterPtr CreateColumnConvert(
    const NTableClient::TColumnSchema& columnSchema,
    int columnIndex) {
    switch (columnSchema.GetWireType()) {
    case EValueType::Int64:
        return CreateInt64ColumnConverter(columnIndex, columnSchema);

    case EValueType::Uint64:
        return CreateUint64ColumnConverter(columnIndex, columnSchema);

    case EValueType::Double:
        switch (columnSchema.CastToV1Type()) {
            case NTableClient::ESimpleLogicalValueType::Float:
                return CreateFloatingPoint32ColumnConverter(columnIndex, columnSchema);
            default:
                return CreateFloatingPoint64ColumnConverter(columnIndex, columnSchema);
        }

    case EValueType::String:
        return CreateStringConverter(columnIndex, columnSchema);

    case EValueType::Boolean:
        return CreateBooleanColumnConverter(columnIndex, columnSchema);

    case EValueType::Any:
        return CreateAnyConverter(columnIndex, columnSchema);

    case EValueType::Composite:
        return CreateCompositeConverter(columnIndex, columnSchema);

    case EValueType::Null:
        return CreateNullConverter();

    case EValueType::Min:
    case EValueType::TheBottom:
    case EValueType::Max:
        break;
    }
    ThrowUnexpectedValueType(columnSchema.GetWireType());
}

////////////////////////////////////////////////////////////////////////////////


TConvertedColumnRange ConvertRowsToColumns(
    TRange<NTableClient::TUnversionedRow> rows,
    const std::vector<NTableClient::TColumnSchema> &columnSchema)
{
    TConvertedColumnRange convertedColumnsRange;
    for (int columnId = 0; columnId < std::ssize(columnSchema); columnId++) {
        auto converter = CreateColumnConvert(columnSchema[columnId], columnId);
        auto columns = converter->Convert(rows);
        convertedColumnsRange.push_back(columns);
    }
    return convertedColumnsRange;
}

////////////////////////////////////////////////////////////////////////////////

}
