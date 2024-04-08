#include "arrow.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/assert/assert.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/io/memory.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/api.h>

namespace NYT {

namespace {

////////////////////////////////////////////////////////////////////////////////

NTi::TTypePtr GetYTType(const std::shared_ptr<arrow::DataType>& arrowType)
{
    switch (arrowType->id()) {
        case arrow::Type::type::BOOL:
            return NTi::Bool();

        case arrow::Type::type::UINT8:
            return NTi::Uint8();
        case arrow::Type::type::UINT16:
            return NTi::Uint16();
        case arrow::Type::type::UINT32:
            return NTi::Uint32();
        case arrow::Type::type::UINT64:
            return NTi::Uint64();

        case arrow::Type::type::INT8:
            return NTi::Int8();
        case arrow::Type::type::INT16:
            return NTi::Int16();
        case arrow::Type::type::DATE32:
        case arrow::Type::type::TIME32:
        case arrow::Type::type::INT32:
            return NTi::Int32();
        case arrow::Type::type::DATE64:
        case arrow::Type::type::TIMESTAMP:
        case arrow::Type::type::INT64:
        case arrow::Type::type::TIME64:
            return NTi::Int64();

        case arrow::Type::type::HALF_FLOAT:
        case arrow::Type::type::FLOAT:
            return NTi::Float();
        case arrow::Type::type::DOUBLE:
            return NTi::Double();

        case arrow::Type::type::STRING:
        case arrow::Type::type::BINARY:
        case arrow::Type::type::FIXED_SIZE_BINARY:
            return NTi::String();

        case arrow::Type::type::LIST:
            return NTi::List(
                GetYTType(std::reinterpret_pointer_cast<arrow::ListType>(arrowType)->value_type()));

        case arrow::Type::type::MAP:
            return NTi::Dict(
                GetYTType(std::reinterpret_pointer_cast<arrow::MapType>(arrowType)->key_type()),
                GetYTType(std::reinterpret_pointer_cast<arrow::MapType>(arrowType)->item_type()));

        default:
            THROW_ERROR_EXCEPTION("Unsupported arrow type: %v", arrowType->ToString());
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateYTTableSchemaFromArrowSchema(const std::shared_ptr<arrow::Schema>& arrowSchema)
{
    TTableSchema resultSchema;
    for (const auto& field : arrowSchema->fields()) {
        auto ytType = NTi::Optional(GetYTType(field->type()));
        resultSchema.AddColumn(TColumnSchema().Name(TString(field->name())).TypeV3(ytType));
    }
    return resultSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
