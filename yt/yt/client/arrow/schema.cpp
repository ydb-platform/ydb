#include "schema.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NArrow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow20::Field>& arrowType);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow20::DataType>& arrowType)
{
    using namespace NTableClient;
    switch (arrowType->id()) {
        case arrow20::Type::type::NA:
            return SimpleLogicalType(ESimpleLogicalValueType::Null);
        case arrow20::Type::type::BOOL:
            return SimpleLogicalType(ESimpleLogicalValueType::Boolean);
        case arrow20::Type::type::UINT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case arrow20::Type::type::UINT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case arrow20::Type::type::UINT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case arrow20::Type::type::UINT64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case arrow20::Type::type::INT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case arrow20::Type::type::INT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case arrow20::Type::type::DATE32:
        case arrow20::Type::type::TIME32:
        case arrow20::Type::type::INT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case arrow20::Type::type::DATE64:
        case arrow20::Type::type::TIMESTAMP:
        case arrow20::Type::type::INT64:
        case arrow20::Type::type::TIME64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case arrow20::Type::type::HALF_FLOAT:
        case arrow20::Type::type::FLOAT:
            return SimpleLogicalType(ESimpleLogicalValueType::Float);
        case arrow20::Type::type::DOUBLE:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case arrow20::Type::type::STRING:
        case arrow20::Type::type::LARGE_STRING:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case arrow20::Type::type::BINARY:
        case arrow20::Type::type::LARGE_BINARY:
        case arrow20::Type::type::FIXED_SIZE_BINARY:
            return SimpleLogicalType(ESimpleLogicalValueType::Any);
        case arrow20::Type::type::LIST:
            return ListLogicalType(
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow20::ListType>(arrowType)->value_field()));
        case arrow20::Type::type::MAP:
            return DictLogicalType(
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow20::MapType>(arrowType)->key_field()),
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow20::MapType>(arrowType)->item_field()));

        case arrow20::Type::type::STRUCT:
        {
            auto structType = std::reinterpret_pointer_cast<arrow20::StructType>(arrowType);
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
        case arrow20::Type::type::DECIMAL128:
        {
            constexpr int MaximumYTDecimalPrecision = 35;
            auto decimalType = std::reinterpret_pointer_cast<arrow20::Decimal128Type>(arrowType);
            if (decimalType->precision() <= MaximumYTDecimalPrecision) {
                return DecimalLogicalType(decimalType->precision(), decimalType->scale());
            } else {
                return SimpleLogicalType(ESimpleLogicalValueType::String);
            }
        }
        case arrow20::Type::type::DECIMAL256:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        default:
            THROW_ERROR_EXCEPTION("Unsupported arrow type: %Qv", arrowType->ToString());
    }
}

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow20::Field>& arrowField)
{
    auto resultType = GetLogicalTypeFromArrowType(arrowField->type());
    return arrowField->nullable() ? OptionalLogicalType(resultType) : resultType;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow20::Schema>& arrowSchema)
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
