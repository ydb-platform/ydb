#pragma once

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TValueExample
{
    TLogicalTypePtr LogicalType;
    NNamedValue::TNamedValue::TValue Value;
    TString PrettyYson;

    TValueExample(TLogicalTypePtr logicalType, NNamedValue::TNamedValue::TValue value, TString prettyYson);
};

std::vector<TValueExample> GetPrimitiveValueExamples();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
