#include "row_helpers.h"

#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static void EnsureTypesMatch(EValueType expected, EValueType actual)
{
    if (expected != actual) {
        THROW_ERROR_EXCEPTION("Unexpected type of TUnversionedValue: expected %Qlv, actual %Qlv",
            expected,
            actual);
    }
}

i64 GetInt64(const TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::Int64, value.Type);
    return value.Data.Int64;
}

ui64 GetUint64(const TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::Uint64, value.Type);
    return value.Data.Uint64;
}

double GetDouble(const NTableClient::TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::Double, value.Type);
    return value.Data.Double;
}

bool GetBoolean(const TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::Boolean, value.Type);
    return value.Data.Boolean;
}

TString GetString(const TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::String, value.Type);
    return value.AsString();
}

NYTree::INodePtr GetAny(const NTableClient::TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::Any, value.Type);
    return NYTree::ConvertToNode(NYson::TYsonString(value.AsString()));
}

NYTree::INodePtr GetComposite(const NTableClient::TUnversionedValue& value)
{
    EnsureTypesMatch(EValueType::Composite, value.Type);
    return NYTree::ConvertToNode(NYson::TYsonString(value.AsString()));
}

bool IsNull(const NTableClient::TUnversionedValue& value)
{
    return value.Type == EValueType::Null;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
