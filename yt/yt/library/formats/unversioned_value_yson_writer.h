#pragma once

#include <yt/yt/client/formats/public.h>
#include <yt/yt/client/formats/config.h>

#include <yt/yt/client/complex_types/yson_format_conversion.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TUnversionedValueYsonWriter
{
public:
    TUnversionedValueYsonWriter(
        const NTableClient::TNameTablePtr& nameTable,
        const NTableClient::TTableSchemaPtr& tableSchema,
        const NComplexTypes::TYsonConverterConfig& config);

    void WriteValue(const NTableClient::TUnversionedValue& value, NYson::IYsonConsumer* consumer);

private:
    THashMap<int, NComplexTypes::TYsonServerToClientConverter> ColumnConverters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
