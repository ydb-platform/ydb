#include "unversioned_value_yson_writer.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_value.h>

#include <yt/yt/core/yson/pull_parser.h>

namespace NYT::NFormats {

using namespace NYson;
using namespace NTableClient;
using namespace NComplexTypes;

////////////////////////////////////////////////////////////////////////////////

TUnversionedValueYsonWriter::TUnversionedValueYsonWriter(
    const TNameTablePtr& nameTable,
    const TTableSchemaPtr& tableSchema,
    const TYsonConverterConfig& config)
{
    if (config.ComplexTypeMode == EComplexTypeMode::Positional &&
        config.DecimalMode == EDecimalMode::Binary &&
        config.TimeMode == ETimeMode::Binary &&
        config.UuidMode == EUuidMode::Binary) {
        return;
    }

    const auto& columns = tableSchema->Columns();
    for (size_t i = 0; i != columns.size(); ++i) {
        const auto& column = columns[i];
        auto id = nameTable->GetIdOrRegisterName(column.Name());
        TComplexTypeFieldDescriptor descriptor(column.Name(), column.LogicalType());
        auto converter = CreateYsonServerToClientConverter(descriptor, config);
        if (converter) {
            ColumnConverters_.emplace(id, std::move(converter));
        }
    }
}

void TUnversionedValueYsonWriter::WriteValue(const TUnversionedValue& value, IYsonConsumer* consumer)
{
    if (value.Type == EValueType::Null) {
        consumer->OnEntity();
        return;
    }
    auto it = ColumnConverters_.find(value.Id);
    if (it != ColumnConverters_.end()) {
        it->second(value, consumer);
        return;
    }
    switch (value.Type) {
        case EValueType::Int64:
            consumer->OnInt64Scalar(value.Data.Int64);
            return;
        case EValueType::Uint64:
            consumer->OnUint64Scalar(value.Data.Uint64);
            return;
        case EValueType::Double:
            consumer->OnDoubleScalar(value.Data.Double);
            return;
        case EValueType::Boolean:
            consumer->OnBooleanScalar(value.Data.Boolean);
            return;
        case EValueType::String:
            consumer->OnStringScalar(value.AsStringBuf());
            return;
        case EValueType::Any:
        case EValueType::Composite:
            consumer->OnRaw(value.AsStringBuf(), EYsonType::Node);
            return;
        case EValueType::Null:
        case EValueType::Min:
        case EValueType::TheBottom:
        case EValueType::Max:
            break;
    }
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
