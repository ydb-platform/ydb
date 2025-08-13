#include "schema.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NArrow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow::Field>& arrowType);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow::DataType>& arrowType)
{
    using namespace NTableClient;
    switch (arrowType->id()) {
        case arrow::Type::NA:
            return SimpleLogicalType(ESimpleLogicalValueType::Null);
        case arrow::Type::BOOL:
            return SimpleLogicalType(ESimpleLogicalValueType::Boolean);
        case arrow::Type::UINT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case arrow::Type::UINT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case arrow::Type::UINT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case arrow::Type::UINT64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case arrow::Type::INT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case arrow::Type::INT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case arrow::Type::DATE32:
        case arrow::Type::TIME32:
        case arrow::Type::INT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case arrow::Type::DATE64:
        case arrow::Type::TIMESTAMP:
        case arrow::Type::INT64:
        case arrow::Type::TIME64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case arrow::Type::HALF_FLOAT:
        case arrow::Type::FLOAT:
            return SimpleLogicalType(ESimpleLogicalValueType::Float);
        case arrow::Type::DOUBLE:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case arrow::Type::BINARY:
        case arrow::Type::LARGE_BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
            return SimpleLogicalType(ESimpleLogicalValueType::Any);
        case arrow::Type::LIST:
            return ListLogicalType(
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow::ListType>(arrowType)->value_field()));
        case arrow::Type::MAP:
            return DictLogicalType(
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow::MapType>(arrowType)->key_field()),
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow::MapType>(arrowType)->item_field()));

        case arrow::Type::STRUCT:
        {
            auto structType = std::reinterpret_pointer_cast<arrow::StructType>(arrowType);
            std::vector<TStructField> members;
            members.reserve(structType->num_fields());
            for (auto fieldIndex = 0; fieldIndex < structType->num_fields(); ++fieldIndex) {
                auto field = structType->field(fieldIndex);
                members.push_back({field->name(), GetLogicalTypeFromArrowType(field)});
            }
            return StructLogicalType(std::move(members));
        }
        // Currently YT supports only Decimal128 with precision <= 35. Thus, we represent short enough arrow decimal types
        // as the corresponding YT decimals, and longer arrow decimal types as strings in decimal form.
        // The latter is subject to change whenever wider decimal types are introduced in YT.
        case arrow::Type::DECIMAL128:
        {
            constexpr int MaximumYTDecimalPrecision = 35;
            auto decimalType = std::reinterpret_pointer_cast<arrow::Decimal128Type>(arrowType);
            if (decimalType->precision() <= MaximumYTDecimalPrecision) {
                return DecimalLogicalType(decimalType->precision(), decimalType->scale());
            } else {
                return SimpleLogicalType(ESimpleLogicalValueType::String);
            }
        }
        case arrow::Type::DECIMAL256:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        default:
            THROW_ERROR_EXCEPTION("Unsupported arrow type: %Qv", arrowType->ToString());
    }
}

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow::Field>& arrowField)
{
    auto resultType = GetLogicalTypeFromArrowType(arrowField->type());
    return arrowField->nullable() ? OptionalLogicalType(resultType) : resultType;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow::Schema>& arrowSchema)
{
    std::vector<TColumnSchema> columns;
    columns.reserve(arrowSchema->fields().size());
    for(const auto& field : arrowSchema->fields()) {
        columns.push_back(TColumnSchema(field->name(), GetLogicalTypeFromArrowType(field)));
    }

    return New<TTableSchema>(
        std::move(columns),
        /*strict*/ true,
        /*uniqueKeys*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
